# imsgy-go

基于go封装的一款db

使用方法:
引用libs包
import (
	. "base_api_go/tbkt/libs" // .引用可以省去前缀
)
1. 单条数据查询Get()方法:
AccountData := DB("base").Table("auth_account").Filter("phone=?", PhoneNumber).Get()
2. 多条数据查询All()方法
UserData := DB("base").Table("auth_user").Filter("account_id=? AND type=? AND status<>?", AccountId, UserType, 2).Select(
"account_id", "id AS user_id", "last_login", "real_pwd", "status").All()
3. 创建记录Create方法:
4. 更新记录Update方法:

注: Filter入参即为where条件
