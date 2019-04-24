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
