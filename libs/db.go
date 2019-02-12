package libs

// 引入go插件
import (
	"base_api_go/pro/config"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// 设置DB全局变量
var db *sql.DB
// 初始化数据库链接
func Pool() *DBPool{
	DBProxy := new(DBPool)
	return DBProxy
}

// 数据库链接结构体
type MysqlDB struct {}

// 数据库操作处理结构体
type DBPool struct {
	MysqlConn *sql.DB	`数据库链接池`
	TableName	string	`数据表名字`
	SelectCondition	[]interface{}	`选择条件`
	WhereCondition	[]map[string]interface{}	`查询条件`
	GroupCondition	[]interface{}	`分组条件`
	OrderCondition	[]interface{}	`排序条件`
}

type retryStop struct {error}

// 数据链接失败重试
func dbRetry(retryNum int, sleepTime time.Duration, DbConnect string) (*sql.DB, error) {
	// 链接数据库
	db, _ := sql.Open("mysql", DbConnect)
	// 检测数据库连接状态
	if err := db.Ping(); err != nil {
		// 判断重试状态
		if s, ok := err.(retryStop); ok {
			return db, s.error
		}else {
			if retryNum--; retryNum > 0 {
				log.Println("数据库连接异常,正在尝试连接")
				time.Sleep(sleepTime)	// 休眠1s中重试链接
				return dbRetry(retryNum, 2*sleepTime, DbConnect)
			}
			return db, err
		}
	}
	// 链接正常直接返回
	return db, nil
}

func (m *MysqlDB)GetDbConn(DBName string) *sql.DB{
	DbConnect := config.DBConfig[DBName]
	// 链接失败进行重试,最多重试3次
	db, _ := dbRetry(3, 1*time.Second, DbConnect)
	// 设置连接池最大链接数--不能大于数据库设置的最大链接数
	db.SetMaxOpenConns(100)
	// 设置最大空闲链接数--小于设置的链接数
	db.SetMaxIdleConns(5)
	// 设置数据库链接超时时间--不能大于数据库设置的超时时间
	db.SetConnMaxLifetime(time.Second * 5)
	return db
}

// 获取数据库链接
func(p *DBPool)DB(DBName string) *DBPool{
	DbConn := new(MysqlDB)
	p.MysqlConn = DbConn.GetDbConn(DBName)
	return p
}

// 查询数据表获取
func(p *DBPool)Table(name string) *DBPool{
	p.TableName = name
	return p
}

// 查询select条件入参,入参类似python的args
func(p *DBPool)Select(params ...interface{}) *DBPool{
	p.SelectCondition = params
	return p
}

// 查询where条件入参,入参类似于python的args
func(p *DBPool)Filter(query interface{}, values ...interface{}) *DBPool{
	p.WhereCondition = append(p.WhereCondition, map[string]interface{}{"query": query, "args": values})
	return p
}

// 定义数据库分组函数,入参类似于python的args
func(p *DBPool)GroupBy(params ...interface{}) *DBPool{
	p.GroupCondition = params
	return p
}

// 定义数据库排序函数,入参类似于python的args
func(p *DBPool)OrderBy(params ...interface{}) *DBPool{
	p.OrderCondition = params
	return p
}

// SQL拼接处理
func (p *DBPool)Sql() string{
	// 匿名函数interface转slice--需要时调用
	fn := func (arr interface{}) []interface{} {
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
	SelectFilter := ""
	for _, vs := range p.SelectCondition{
		if SelectFilter == "" {
			SelectFilter += fmt.Sprintf("%s", vs)
		}else {
			SelectFilter += fmt.Sprintf(",%s", vs)
		}
	}
	// 没有设置获取数据字段,默认查询全部
	if SelectFilter == "" {
		SelectFilter = "*"
	}
	// 处理where条件
	WhereFilter := ""
	if len(p.WhereCondition[0]) > 0{
		FilterList := strings.Split(p.WhereCondition[0]["query"].(string), "AND")
		// 匿名函数处理where入参, interface转slice
		WhereList := fn(p.WhereCondition[0]["args"])
		// 组合where条件
		for index, value := range FilterList{
			// 参数分割,并去除空格
			NewValue := strings.TrimSpace(strings.Split(value, "?")[0])
			WhereValue := WhereList[index]
			// 入参类型断言
			switch reflect.ValueOf(WhereValue).Kind(){
				case reflect.Int:
					if WhereFilter == "" {
						WhereFilter += fmt.Sprintf("WHERE %v%d", NewValue, WhereList[index])
					}else{
						WhereFilter += fmt.Sprintf(" AND %v%d", NewValue, WhereList[index])
					}
				case reflect.String:
					if WhereFilter == "" {
						WhereFilter += fmt.Sprintf("WHERE %v'%v'", NewValue, WhereList[index])
					}else{
						WhereFilter += fmt.Sprintf(" AND %v'%v'", NewValue, WhereList[index])
					}
				case reflect.Slice:
					// 匿名函数处理where入参, interface转slice
					NewList := fn(WhereValue)
					FilterWhere := ""
					for v := range NewList{
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
					}else{
						WhereFilter += fmt.Sprintf(" AND %v (%v)", NewValue, FilterWhere)
					}
				default:
					panic("1002:The Params Valid")
			}
		}
	}
	// 处理分组条件
	GroupFilter := ""
	for _, vg := range p.GroupCondition{
		if GroupFilter == "" {
			GroupFilter += fmt.Sprintf("GROUP BY %s", vg)
		}else {
			GroupFilter += fmt.Sprintf(",%s", vg)
		}
	}
	// 处理排序条件
	OrderFilter := ""
	for _, vo := range p.OrderCondition{
		if OrderFilter == "" {
			OrderFilter += fmt.Sprintf("ORDER BY %s", vo)
		}else {
			OrderFilter += fmt.Sprintf(",%s", vo)
		}
	}
	// 格式化生成SQL
	Sql := fmt.Sprintf("SELECT %v FROM %v %v ", SelectFilter, p.TableName, WhereFilter)
	return Sql
}

// 数据库返回数据处理,返回数据类型为slice,slice内层为map
func dealMysqlRows(rows *sql.Rows) []map[string]interface{}{
	defer rows.Close()
	// 获取列名
	columns, err := rows.Columns()
	columnTypes, _ := rows.ColumnTypes()
	// 获取每列的数据类型
	ColumnTypeMap := make(map[string]string)
	for _, v := range columnTypes{
		ColumnTypeMap[v.Name()] = v.DatabaseTypeName()
	}
	CheckError(err, 0)
	// 定义返回参数的slice
	retValues := make([]sql.RawBytes, len(columns))
	// 定义数据列名的slice
	scanArgs := make([]interface{}, len(retValues))
	// 数据列赋值
	for i := range retValues{
		scanArgs[i] = &retValues[i]
	}
	// 定义返回数据类型slice
	var resList []map[string]interface{}
	// 返回数据赋值
	for rows.Next()  {
		// 检测数据列是否超出
		err = rows.Scan(scanArgs...)
		CheckError(err, 0)
		// 内层数据格式
		rowMap := make(map[string]interface{})
		for i, colVal := range retValues{
			if colVal != nil{
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
func(p *DBPool) Get() map[string]interface{}{
	RetOne := make(map[string]interface{})
	// 数据库操作结束,释放链接
	defer p.MysqlConn.Close()
	GetSql := p.Sql() + "LIMIT 1"
	rows, err := p.MysqlConn.Query(GetSql)
	CheckError(err, 0)
	// 数据获取
	RetMap := dealMysqlRows(rows)
	if len(RetMap) > 0{
		RetOne = RetMap[0]
	}
	return RetOne
}

// 获取多条数据,返回数据类型为slice,slice内层为map
func(p *DBPool) All() []map[string]interface{}{
	// 数据库操作结束,释放链接
	defer p.MysqlConn.Close()
	GetSql := p.Sql()
	rows, err := p.MysqlConn.Query(GetSql)
	CheckError(err, 0)
	// 数据获取
	RetMap := dealMysqlRows(rows)
	return RetMap
}

// 定义创建数据方法,返回最后的ID
func(p *DBPool) Create(params map[string]interface{}) int{
	// 数据库操作结束,释放链接
	defer p.MysqlConn.Close()
	// 自定待创建的函数和参数
	InsertCols, InsertArgs := "", ""
	for k, v := range params{
		// 数据列只能为string类型
		if InsertCols == "" {
			InsertCols += fmt.Sprintf("%s", k)
		}else {
			InsertCols += fmt.Sprintf(",%s", k)
		}
		// 判断数据类型,类型断言判断
		switch v.(type) {
			case int:
				if InsertArgs == "" {
					InsertArgs += fmt.Sprintf("%d", v)
				}else {
					InsertArgs += fmt.Sprintf(",%d", v)
				}
			case string:
				if InsertArgs == "" {
					InsertArgs += fmt.Sprintf("'%s'", v)
				}else{
					InsertArgs += fmt.Sprintf(",'%s'", v)
				}
			case float64:
				if InsertArgs == "" {
					InsertArgs += fmt.Sprintf("%f", v)
				}else {
					InsertArgs += fmt.Sprintf(",%f", v)
				}
		}
	}
	// 开启MySql事务
	tx, err := p.MysqlConn.Begin()
	CheckError(err, 1)
	// 组合数据写入SQL
	InsertSql := fmt.Sprintf("INSERT INTO %v(%v) VALUES (%v);", p.TableName, InsertCols, InsertArgs)
	retData, err := p.MysqlConn.Exec(InsertSql)
	CheckError(err, 0)
	LastId, err := retData.LastInsertId()
	if err != nil{
		log.Println("数据创建失败,事务回滚")
		tx.Rollback()
	}
	tx.Commit()
	return int(LastId)
}


// 定义更新数据方法,返回影响的行数
func(p *DBPool) Update(params map[string]interface{}) int{
	// 数据库操作结束,释放链接
	defer p.MysqlConn.Close()
	// 处理where条件
	WhereFilter := ""
	for _, vw := range p.WhereCondition{
		if WhereFilter == "" {
			WhereFilter += fmt.Sprintf("%s", vw)
		}else {
			WhereFilter += fmt.Sprintf(" AND %s", vw)
		}
	}
	// 定义待创建的函数和参数
	UpdateArgs := ""
	for k, v := range params{
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
				}
		}else {
			// 判断数据类型,类型断言判断
			switch v.(type) {
				case int:
					UpdateArgs += fmt.Sprintf(",%s=%d", k, v)
				case string:
					UpdateArgs += fmt.Sprintf(",%s='%s'", k, v)
				case float64:
					UpdateArgs += fmt.Sprintf(",%s=%f", k, v)
				}
		}
	}
	// 组合数据更新SQL
	UpdateSql := fmt.Sprintf("UPDATE %v SET %v WHERE %v;", p.TableName, UpdateArgs, WhereFilter)
	// 开启MySql事务
	tx, err := p.MysqlConn.Begin()
	CheckError(err, 1)
	retData, err := p.MysqlConn.Exec(UpdateSql)
	CheckError(err, 1)
	ARows, err := retData.RowsAffected()
	if err != nil{
		log.Println("数据更新失败,事务回滚")
		tx.Rollback()
	}
	// 提交事务
	tx.Commit()
	return int(ARows)
}

// 定义删除数据方法
func(p *DBPool) Delete() int{
	// 数据库操作结束,释放链接
	defer p.MysqlConn.Close()
	// 处理where条件
	WhereFilter := ""
	for _, vw := range p.WhereCondition{
		if WhereFilter == "" {
			WhereFilter += fmt.Sprintf("%s", vw)
		}else {
			WhereFilter += fmt.Sprintf(" AND %s", vw)
		}
	}
	// 组合删除数据SQL
	DeleteSql := fmt.Sprintf("DELETE FROM %v WHERE %v", p.TableName, WhereFilter)
	// 开启MySql事务
	tx, err := p.MysqlConn.Begin()
	retData, err := p.MysqlConn.Exec(DeleteSql)
	CheckError(err, 0)
	ARows, err := retData.RowsAffected()
	if err != nil{
		log.Println("数据删除失败,事务回滚")
		tx.Rollback()
	}
	// 提交事务
	tx.Commit()
	return int(ARows)
}


// 查询执行SQL方法
func(p *DBPool) Execute(Sql string) int{
	// 数据库操作结束,释放链接
	defer p.MysqlConn.Close()
	retData, err := p.MysqlConn.Exec(Sql)
	// 开启MySql事务
	tx, err := p.MysqlConn.Begin()
	CheckError(err, 0)
	ARows, err := retData.RowsAffected()
	if err != nil{
		log.Println("数据库执行失败,事务回滚")
		tx.Rollback()
	}
	// 提交事务
	tx.Commit()
	return int(ARows)
}

// 定义执行SQL返回一条数据方法
func(p *DBPool) FetchOne(Sql string) map[string]interface{}{
	// 数据库操作结束,释放链接
	defer p.MysqlConn.Close()
	rows, err := p.MysqlConn.Query(Sql)
	CheckError(err, 0)
	// 数据获取
	RetMap := dealMysqlRows(rows)
	return RetMap[0]
}

// 定义执行SQL返回多条数据方法
func(p *DBPool) FetchAll(Sql string) []map[string]interface{}{
	// 数据库操作结束,释放链接
	defer p.MysqlConn.Close()
	rows, err := p.MysqlConn.Query(Sql)
	CheckError(err, 0)
	// 数据获取
	RetMap := dealMysqlRows(rows)
	return RetMap
}
