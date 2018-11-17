package main

import (
	"fmt"
	"os"

	"github.com/go-apibox/api"
)

func main() {
	app, err := api.NewApp()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	app.Run(apiRoutes)
}
