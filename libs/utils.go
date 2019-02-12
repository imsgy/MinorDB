package libs

import (
	"base_api_go/pro/config"
	"encoding/base64"
	"fmt"
	"github.com/gin-gonic/gin"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)


// 定义异常函数,默认直接传0即可
func CheckError(err error, eType int){
	if err != nil {
		if eType == 1{
			log.Fatal("程序异常中止运行:", err.Error())
		}else{
			log.Println("程序异常:", err.Error())
		}
	}
}

// 判断数据是否存在于slice中
func InSlice(sList [] interface{}, st interface{}) bool {
	for _, a := range sList {
		if a == st {
			return true
		}
	}
	return false
}

// 定义成功返回方法
func JsonOk(c *gin.Context, data interface{}){
	c.JSON(http.StatusOK, gin.H{
		"response": "ok",
		"data": data,
		"message": "",
	})
}

// 定义失败返回方法
func JsonFail(c *gin.Context, message string){
	c.Abort()
	c.JSON(http.StatusOK, gin.H{
		"response": "ok",
		"data": "",
		"message": message,
	})
}

// 定义生成token的方法
func CreateToken(AccountId, UserId int) string{
	ExpireTime := int(time.Now().Unix()) + config.SessionCookieAge
	Code := AccountId ^ UserId ^ ExpireTime ^ config.SecretKey
	SToken := fmt.Sprintf("%d|%d|%d|%d", AccountId, UserId, ExpireTime, Code)
	CToken := StrXor(SToken, config.SecretKey)
	Token := base64.StdEncoding.EncodeToString(CToken)
	return Token
}

// 定义token解析方法
func DecodeToken(Token string) map[string]interface{}{
	// 定义返回数据格式
	retToken := make(map[string]interface{})
	if Token == "" {
		return retToken
	}
	// 填充base64被去掉的等号
	tNum := -len(Token) % 4
	tRange := make([]int, tNum)
	for range tRange{
		Token += "="
	}
	// 获取byte类型数据
	BToken, _ := base64.StdEncoding.DecodeString(Token)
	SToken := string(StrXor(string(BToken), config.SecretKey))
	TokenData := strings.Split(SToken, "|")
	// 字符串转换成整型
	AccountId, _ := strconv.Atoi(TokenData[0])
	UserId, _ := strconv.Atoi(TokenData[1])
	ExpireTime, _ := strconv.Atoi(TokenData[2])
	retToken["account_id"], retToken["user_id"], retToken["expire"] = AccountId, UserId, ExpireTime
	return retToken
}


// 定义异或加密方法
func StrXor(strParam string, sKey int) []byte{
	XorKey := sKey & 0xff
	aByte := []byte(strParam)
	bByte := make([]byte, len(aByte))
	for index, value := range aByte{
		dInt := int(value)
		bByte[index] = byte(dInt ^ XorKey)
	}
	return bByte
}
