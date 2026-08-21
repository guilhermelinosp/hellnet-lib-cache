package golanglibtemplate

import (
	"fmt"
)

// ExampleGreet demonstrates the happy path of Greet.
func ExampleGreet() {
	msg, _ := Greet("World")
	fmt.Println(msg)
	// Output: Hello, World!
}
