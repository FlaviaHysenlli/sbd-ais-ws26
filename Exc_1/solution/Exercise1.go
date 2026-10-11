package main

import "fmt"

type GreetingStruct struct {
	Greeting string
}

func (g GreetingStruct) Hello() {
	fmt.Println(g.Greeting)
}

func main() {
	g := GreetingStruct{Greeting: "Hello World!"}
	g.Hello()
}
