package libs

// 引入go插件
import (
	"database/sql"
	"fmt"
	_ "github.com/mysql"
	"reflect"
	"strconv"
	"strings"
	"pro/config"
	"pro/libs"
	"pro/logger"
	"time"
)
// 设置DB全部变量
var db *sql.DB
var connPool map[string]*sql.DB

// 初始化数据库链接map
func init(){
	dbMap := make(map[string]*sql.DB)
	for name, addr := range config.DBConfig{
		var err error
		db, err = sql.Open("mysql", addr)
		// 设置连接池最大链接数--不能大于数据库设置的最大链接数
		db.SetMaxOpenConns(1000)
		// 设置最大空闲链接数--小于设置的链接数
		db.SetMaxIdleConns(10)
		// 设置数据库链接超时时间--不能大于数据库设置的超时时间
		db.SetConnMaxLifetime(time.Second * 5)
		if err != nil {
			logger.Info("链接异常")
		}
		dbMap[name] = db
	}
	connPool = dbMap
}

func DB(name string) *DBPool {
	Pool := new(DBPool)
	Pool.pool = connPool[name]
	return Pool
}

// 数据库操作处理结构体
type DBPool struct {
	pool            *sql.DB                  `数据库连接池`
	tableName       string                   `数据表名字`
	selectCondition []string                 `选择条件`
	whereCondition  []map[string]interface{} `查询条件`
	groupCondition  []string                 `分组条件`
	orderCondition  []string                 `排序条件`
}

// 查询数据表获取
func (p *DBPool) Table(name string) *DBPool {
	p.tableName = name
	return p
}

// 查询select条件入参,入参类似python的args
func (p *DBPool) Select(params ...string) *DBPool {
	p.selectCondition = params
	return p
}

// 查询where条件入参,入参类似于python的args
func (p *DBPool) Filter(query interface{}, values ...interface{}) *DBPool {
	p.whereCondition = append(p.whereCondition, map[string]interface{}{"query": query, "args": values})
	return p
}

// 定义数据库分组函数,入参类似于python的args
func (p *DBPool) GroupBy(params ...string) *DBPool {
	p.groupCondition = params
	return p
}

// 定义数据库排序函数,入参类似于python的args
func (p *DBPool) OrderBy(params ...string) *DBPool {
	p.orderCondition = params
	return p
}

// SQL拼接处理
func (p *DBPool) sql() string {
	// 匿名函数interface转slice--需要时调用
	fn := func(arr interface{}) []interface{} {
		v := reflect.ValueOf(arr)
		if v.Kind() != reflect.Slice {
			panic("The Filter Params Valid")
		}
		vLen := v.Len()
		ret := make([]interface{}, vLen)
		for i := 0; i < vLen; i++ {
			ret[i] = v.Index(i).Interface()
		}
		return ret
	}
	// 处理select条件
	SelectFilter := strings.Join(p.selectCondition, ",")
	// 没有设置获取数据字段,默认查询全部
	if len(p.selectCondition) == 0 {
		SelectFilter = "*"
	}
	// 处理where条件
	WhereFilter := ""
	if len(p.whereCondition[0]) > 0 {
		FilterList := strings.Split(p.whereCondition[0]["query"].(string), "AND")
		// 匿名函数处理where入参, interface转slice
		WhereList := fn(p.whereCondition[0]["args"])
		// 组合where条件
		for index, value := range FilterList {
			// 参数分割,并去除空格
			NewValue := strings.TrimSpace(strings.Split(value, "?")[0])
			WhereValue := WhereList[index]
			// 入参类型断言
			switch reflect.ValueOf(WhereValue).Kind() {
			case reflect.Int:
				if WhereFilter == "" {
					WhereFilter += fmt.Sprintf("WHERE %v%d", NewValue, WhereList[index])
				} else {
					WhereFilter += fmt.Sprintf(" AND %v%d", NewValue, WhereList[index])
				}
			case reflect.String:
				if WhereFilter == "" {
					WhereFilter += fmt.Sprintf("WHERE %v'%v'", NewValue, WhereList[index])
				} else {
					WhereFilter += fmt.Sprintf(" AND %v'%v'", NewValue, WhereList[index])
				}
			case reflect.Slice:
				// 匿名函数处理where入参, interface转slice
				NewList := fn(WhereValue)
				FilterWhere := ""
				for v := range NewList {
					switch reflect.ValueOf(v).Kind() {
					case reflect.Int:
						if FilterWhere == "" {
							FilterWhere += fmt.Sprintf("%d", v)
						} else {
							FilterWhere += fmt.Sprintf(",%d", v)
						}
					case reflect.String:
						if FilterWhere == "" {
							FilterWhere += fmt.Sprintf("'%v'", v)
						} else {
							FilterWhere += fmt.Sprintf(",'%v'", v)
						}
					default:
						panic("1001:The Params Valid")
					}
				}
				if WhereFilter == "" {
					WhereFilter += fmt.Sprintf("WHERE %v (%v)", NewValue, FilterWhere)
				} else {
					WhereFilter += fmt.Sprintf(" AND %v (%v)", NewValue, FilterWhere)
				}
			default:
				panic("1002:The Params Valid")
			}
		}
	}
	// 处理分组条件
	GroupFilter := strings.Join(p.groupCondition, ",")
	// 处理排序条件
	OrderFilter := strings.Join(p.orderCondition, ",")
	// 格式化生成SQL
	Sql := fmt.Sprintf("SELECT %v FROM %v %v %v %v", SelectFilter, p.tableName, WhereFilter, GroupFilter, OrderFilter)
	return Sql
}

// 数据库返回数据处理,返回数据类型为slice,slice内层为map
func dealMysqlRows(rows *sql.Rows) []map[string]interface{} {
	defer closeRows(rows)
	// 获取列名
	columns, err := rows.Columns()
	columnTypes, _ := rows.ColumnTypes()
	// 获取每列的数据类型
	ColumnTypeMap := make(map[string]string)
	for _, v := range columnTypes {
		ColumnTypeMap[v.Name()] = v.DatabaseTypeName()
	}
	libs.CheckError(err, 0)
	// 定义返回参数的slice
	retValues := make([]sql.RawBytes, len(columns))
	// 定义数据列名的slice
	scanArgs := make([]interface{}, len(retValues))
	// 数据列赋值
	for i := range retValues {
		scanArgs[i] = &retValues[i]
	}
	// 定义返回数据类型slice
	var resList []map[string]interface{}
	// 返回数据赋值
	for rows.Next() {
		// 检测数据列是否超出
		err = rows.Scan(scanArgs...)
		libs.CheckError(err, 0)
		// 内层数据格式
		rowMap := make(map[string]interface{})
		for i, colVal := range retValues {
			if colVal != nil {
				keyName := columns[i]
				value := string(colVal)
				// 数据类型转换
				switch ColumnTypeMap[keyName] {
				case "INT":
					newValue, _ := strconv.Atoi(value)
					rowMap[keyName] = newValue
				case "TINYINT":
					newValue, _ := strconv.Atoi(value)
					rowMap[keyName] = newValue
				case "VARCHAR":
					rowMap[keyName] = value
				case "DATETIME":
					newValue, _ := time.Parse(value, value)
					rowMap[keyName] = newValue
				default:
					rowMap[keyName] = value
				}
			}
		}
		resList = append(resList, rowMap)
	}
	return resList
}

// 获取第一条数据,返回数据类型为map
func (p *DBPool) Get() map[string]interface{} {
	var RetOne map[string]interface{}
	GetSql := p.sql() + "LIMIT 1"
	rows, err := p.pool.Query(GetSql)
	libs.CheckError(err, 0)
	// 数据获取
	RetMap := dealMysqlRows(rows)
	if len(RetMap) > 0 {
		RetOne = RetMap[0]
	}
	return RetOne
}

// 获取多条数据,返回数据类型为slice,slice内层为map
func (p *DBPool) All() []map[string]interface{} {
	GetSql := p.sql()
	rows, err := p.pool.Query(GetSql)
	libs.CheckError(err, 0)
	// 数据获取
	RetMap := dealMysqlRows(rows)
	return RetMap
}

// 定义创建数据方法,返回最后的ID
func (p *DBPool) Create(params map[string]interface{}) (lastId int, err error) {
	// 自定待创建的函数和参数
	InsertCols, InsertArgs := "", ""
	for k, v := range params {
		// 数据列只能为string类型
		if InsertCols == "" {
			InsertCols += fmt.Sprintf("%s", k)
		} else {
			InsertCols += fmt.Sprintf(",%s", k)
		}
		// 判断数据类型,类型断言判断
		switch v.(type) {
		case int:
			if InsertArgs == "" {
				InsertArgs += fmt.Sprintf("%d", v)
			} else {
				InsertArgs += fmt.Sprintf(",%d", v)
			}
		case string:
			if InsertArgs == "" {
				InsertArgs += fmt.Sprintf("'%s'", v)
			} else {
				InsertArgs += fmt.Sprintf(",'%s'", v)
			}
		case float64:
			if InsertArgs == "" {
				InsertArgs += fmt.Sprintf("%f", v)
			} else {
				InsertArgs += fmt.Sprintf(",%f", v)
			}
		default:
			if InsertArgs == "" {
				InsertArgs += fmt.Sprintf("%v", v)
			} else {
				InsertArgs += fmt.Sprintf(",%v", v)
			}
		}
	}
	// 组合数据写入SQL
	InsertSql := fmt.Sprintf("INSERT INTO %v(%v) VALUES (%v);", p.tableName, InsertCols, InsertArgs)
	retData, err := p.pool.Exec(InsertSql)
	if err != nil {
		return 0, nil
	}
	LastId, err := retData.LastInsertId()
	if err != nil {
		return 0, err
	}
	return int(LastId), err
}

// 定义更新数据方法,返回影响的行数
func (p *DBPool) Update(params map[string]interface{}) (affectRows int, err error) {
	// 处理where条件
	WhereFilter := ""
	for _, vw := range p.whereCondition {
		if WhereFilter == "" {
			WhereFilter += fmt.Sprintf("%s", vw)
		} else {
			WhereFilter += fmt.Sprintf(" AND %s", vw)
		}
	}
	// 定义待创建的函数和参数
	UpdateArgs := ""
	for k, v := range params {
		// 数据列只能为string类型
		if UpdateArgs == "" {
			// 判断数据类型,类型断言判断
			switch v.(type) {
			case int:
				UpdateArgs += fmt.Sprintf("%s=%d", k, v)
			case string:
				UpdateArgs += fmt.Sprintf("%s='%s'", k, v)
			case float64:
				UpdateArgs += fmt.Sprintf("%s=%f", k, v)
			default:
				UpdateArgs += fmt.Sprintf("%v=%v", k, v)
			}
		} else {
			// 判断数据类型,类型断言判断
			switch v.(type) {
			case int:
				UpdateArgs += fmt.Sprintf(",%s=%d", k, v)
			case string:
				UpdateArgs += fmt.Sprintf(",%s='%s'", k, v)
			case float64:
				UpdateArgs += fmt.Sprintf(",%s=%f", k, v)
			default:
				UpdateArgs += fmt.Sprintf(",%v=%v", k, v)
			}
		}
	}
	// 组合数据更新SQL
	UpdateSql := fmt.Sprintf("UPDATE %v SET %v WHERE %v;", p.tableName, UpdateArgs, WhereFilter)
	retData, err := p.pool.Exec(UpdateSql)
	if err != nil {
		return 0, nil
	}
	ARows, err := retData.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(ARows), nil
}

// 定义删除数据方法
func (p *DBPool) Delete() (affectRows int, err error) {
	// 处理where条件
	WhereFilter := ""
	for _, vw := range p.whereCondition {
		if WhereFilter == "" {
			WhereFilter += fmt.Sprintf("%s", vw)
		} else {
			WhereFilter += fmt.Sprintf(" AND %s", vw)
		}
	}
	// 组合删除数据SQL
	DeleteSql := fmt.Sprintf("DELETE FROM %v WHERE %v", p.tableName, WhereFilter)
	retData, err := p.pool.Exec(DeleteSql)
	if err != nil {
		return 0, err
	}
	ARows, err := retData.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(ARows), nil
}

// 查询执行SQL方法
func (p *DBPool) Execute(Sql string) (affectRows int, err error) {
	retData, err := p.pool.Exec(Sql)
	if err != nil{
		return 0, err
	}
	ARows, err := retData.RowsAffected()
	if err != nil{
		return 0, err
	}
	return int(ARows), nil
}

// 定义执行SQL返回一条数据方法
func (p *DBPool) FetchOne(Sql string) map[string]interface{} {
	var RetOne map[string]interface{}
	rows, err := p.pool.Query(Sql)
	libs.CheckError(err, 0)
	// 数据获取
	RetMap := dealMysqlRows(rows)
	if len(RetMap) > 0 {
		RetOne = RetMap[0]
	}
	return RetOne
}

// 定义执行SQL返回多条数据方法
func (p *DBPool) FetchAll(Sql string) []map[string]interface{} {
	rows, err := p.pool.Query(Sql)
	libs.CheckError(err, 0)
	// 数据获取
	RetMap := dealMysqlRows(rows)
	return RetMap
}

// 关闭行,释放链接
func closeRows(r *sql.Rows) {
	err := r.Close()
	if err != nil {
		logger.Info("关闭Rows异常")
	}
}

func(p *DBPool)BulkCreate(params []map[string]interface{})(affectRows int, err error){
	// 自定待创建的函数和参数
	InsertCols, InsertArgsList := "", ""
	for k := range params[0]{
		// 数据列只能为string类型
		if InsertCols == "" {
			InsertCols += fmt.Sprintf("%s", k)
		} else {
			InsertCols += fmt.Sprintf(",%s", k)
		}
	}
	for _, data := range params{
		InsertArgs := ""
		for _, v := range data {
			// 判断数据类型,类型断言判断
			switch v.(type) {
			case int:
				if InsertArgs == "" {
					InsertArgs += fmt.Sprintf("%d", v)
				} else {
					InsertArgs += fmt.Sprintf(",%d", v)
				}
			case string:
				if InsertArgs == "" {
					InsertArgs += fmt.Sprintf("'%s'", v)
				} else {
					InsertArgs += fmt.Sprintf(",'%s'", v)
				}
			case float64:
				if InsertArgs == "" {
					InsertArgs += fmt.Sprintf("%f", v)
				} else {
					InsertArgs += fmt.Sprintf(",%f", v)
				}
			default:
				if InsertArgs == "" {
					InsertArgs += fmt.Sprintf("%v", v)
				} else {
					InsertArgs += fmt.Sprintf(",%v", v)
				}
			}
		}
		if InsertArgs != ""{
			if InsertArgsList == "" {
				InsertArgsList += fmt.Sprintf("(%v)", InsertArgs)
			}else{
				InsertArgsList += fmt.Sprintf(",(%v)", InsertArgs)
			}
		}
	}

	// 组合数据写入SQL
	InsertSql := fmt.Sprintf("INSERT INTO %v(%v) VALUES %v;", p.tableName, InsertCols, InsertArgsList)
	retData, err := p.pool.Exec(InsertSql)
	if err != nil {
		return 0, nil
	}
	ARows, err := retData.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(ARows), nil
}
