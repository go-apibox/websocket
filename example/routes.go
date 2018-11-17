package main

import (
	"github.com/go-apibox/api"
)

var apiRoutes = []*api.Route{
	api.NewRoute("Test", TestAction),
	api.NewRoute("Subscribe", SubscribeAction),
}
