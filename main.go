package main

import (
	"cmp"
	"os"

	"github.com/gin-gonic/gin"
)

func main() {
	router := gin.Default()

	router.GET("/", func(c *gin.Context) {
		c.String(200, "Hello, Go!")
	})

	router.Run(":" + cmp.Or(os.Getenv("PORT"), "3000"))
}

