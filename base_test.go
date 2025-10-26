package mate

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

var rabbit = NewRabbit(
	"172.16.110.206",
	5672,
	"guest",
	"WJbu9KAsp65Z1Yzv",
	"/super",
	30,
	1,
	30,
	nil,
)

type TestConsume struct {
}

func (t *TestConsume) RunConsume() (err error) {
	client := rabbit.NewClient()
	err = client.Use(t).Retry(3).Receive(client.Fanout, "ex_test_exchange", nil, "qx_test_queue")
	return
}

func (t *TestConsume) Process(body []byte) (err error) {
	fmt.Println("Test Running", string(body))
	err = errors.New("test error")
	return
}

func TestMQBase_Run(t *testing.T) {
	//var mqInst = &MQBase{}
	//mqInst.Add(new(TestConsume))
	//mqInst.Blocking().Run()
	client := rabbit.NewClient()
	var (
		ok error

		wg = &sync.WaitGroup{}
	)
	wg.Add(3)

	go func() {
		defer wg.Done()
		defer func() {
			if x := recover(); x != nil {
				fmt.Println(fmt.Sprintf("%#v", x))
			}
		}()
		var i int
		for i < 10000 {
			err := client.Publish(client.Fanout, "ex_test_exchange", "", []byte(fmt.Sprintf("message one:%d", i)))
			if !errors.Is(err, ok) {
				client.log.Error(fmt.Sprintf("Exception:%s", err.Error()))
				t.Errorf("Exception:%s", err.Error())
			}
			i++
		}
	}()

	go func() {
		defer wg.Done()
		defer func() {
			if x := recover(); x != nil {
				fmt.Println(fmt.Sprintf("%#v", x))
			}
		}()
		var i int
		for i < 10000 {
			err := client.Publish(client.Fanout, "ex_test_exchange", "", []byte(fmt.Sprintf("message two:%d", i)))
			if !errors.Is(err, ok) {
				client.log.Error(fmt.Sprintf("Exception:%s", err.Error()))
				t.Errorf("Exception:%s", err.Error())
			}
			i++
		}
	}()

	go func() {
		defer wg.Done()
		defer func() {
			if x := recover(); x != nil {
				fmt.Println(fmt.Sprintf("%#v", x))
			}
		}()
		var i int
		for i < 10000 {
			err := client.Publish(client.Fanout, "ex_test_exchange", "", []byte(fmt.Sprintf("message three:%d", i)))

			if !errors.Is(err, ok) {
				client.log.Error(fmt.Sprintf("Exception:%s", err.Error()))
				t.Errorf("Exception:%s", err.Error())
			}
			i++
		}
	}()

	wg.Wait()
}

func TestMQBase_Consume(t *testing.T) {
	var mqInst = &MQBase{}
	mqInst.Add(new(TestConsume))
	mqInst.Blocking().Run()
}
