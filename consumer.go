package mate

import (
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"reflect"
	"sync"
	"time"
)

func (c *Client) Receive(exchangeType ExType, exchangeName string, routeKeys []string, queueName string) (err error) {
	var (
		conn *amqp.Connection
	)
	defer func() {
		if c.connect != nil {
			c.connections.Put(c.connect)
		}
	}()
	for {
		//连接服务
		err = c.connection()
		if err != nil {
			return
		}
		conn = c.conn
		//无处理逻辑退出
		if c.proc == nil {
			err = errors.New("please implement the processing method")
			return
		}
		//获取MQ对象
		mcName := reflect.TypeOf(c.proc).Elem().Name()

		//模式不对退出
		if exchangeType != "topic" && exchangeType != "direct" && exchangeType != "fanout" {
			err = errors.New("other modes are not supported")
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
			return
		}

		if exchangeType == "fanout" {
			routeKeys = []string{""}
		}

		//open channel 如果失败重连

		if c.consumerNum > 0 {
			var consumerWg = &sync.WaitGroup{}
			consumerWg.Add(c.consumerNum)
			for i := 0; i < c.consumerNum; i++ {
				go c.goConsumer(i, consumerWg, conn, exchangeType, exchangeName, queueName, routeKeys, mcName)
			}
			consumerWg.Wait()
		}

		var restartLog = fmt.Sprintf("[MQ] [CONSUMER] [%s] [RESTART] Internal Restart...", mcName)
		fmt.Println(restartLog)
	}
	return
}

func (c *Client) Sentry(conn *amqp.Connection, ch *amqp.Channel, forever chan struct{}) {
	var gap = time.Second * 10
	var timer = time.NewTimer(gap)
	for {
		select {
		case <-timer.C:
			if conn.IsClosed() || ch.IsClosed() {
				forever <- struct{}{}
				return
			}
			timer.Reset(gap)
		}
	}
}

func (c *Client) AckMessage(conn *amqp.Connection, ch *amqp.Channel, msg *amqp.Delivery, mcName string, tag string) (connNormal bool) {
	if !conn.IsClosed() && !ch.IsClosed() {
		connNormal = true
		var num = 3
		if tag != "" {
			tag = fmt.Sprintf("[%s] ", tag)
		}
		for i := 0; i < num; i++ {
			err := msg.Ack(true)
			if err != nil {
				c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] %s[ACK] [FAIL] Times:%d, Message: %s, Exception: %s", mcName, tag, i+1, string(msg.Body), err.Error()))
				continue
			} else {
				c.log.Info(fmt.Sprintf("[MQ] [CONSUMER] [%s] %s[ACK] [OK] Times:%d, Message: %s", mcName, tag, i+1, string(msg.Body)))
				return
			}
		}
	}
	return
}

func (c *Client) DelayReceive(exchangeType ExType, exchangeName string, routeKeys []string, queueName string) (err error) {

	defer func() {
		if c.connect != nil {
			c.connections.Put(c.connect)
		}
	}()
	for {
		err = c.connection()
		if err != nil {
			return
		}
		if c.proc == nil {
			err = errors.New("please implement the processing method")
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] Exception:%s", err.Error()))
			return
		}
		mcName := reflect.TypeOf(c.proc).Elem().Name()

		if exchangeType != "topic" && exchangeType != "direct" && exchangeType != "fanout" {
			err = errors.New("other modes are not supported")
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
			return
		}

		if exchangeType == "fanout" {
			routeKeys = []string{""}
		}

		if c.consumerNum > 0 {
			var consumerWg = &sync.WaitGroup{}
			consumerWg.Add(c.consumerNum)
			for i := 0; i < c.consumerNum; i++ {
				go c.goDelayConsumer(i, consumerWg, c.conn, exchangeType, exchangeName, queueName, routeKeys, mcName)
			}
			consumerWg.Wait()
		}

		var restartLog = fmt.Sprintf("[MQ] [CONSUMER] [%s] [RESTART] Internal Restart...", mcName)
		fmt.Println(restartLog)
	}

}

func (c *Client) goConsumer(id int, wg *sync.WaitGroup, conn *amqp.Connection, exchangeType ExType, exchangeName string, queueName string, routes []string, mcName string) {
	var (
		err      error
		n        int
		messages <-chan amqp.Delivery
		msgItem  *amqp.Delivery
		ch       *amqp.Channel
		queue    amqp.Queue
	)
	defer wg.Done()
	defer func() {
		if xy := recover(); xy != nil {
			Exception := fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] [PANIC] Msg:%s, Exception:%#v", mcName, id, string(msgItem.Body), xy)
			c.log.Error(Exception)
			//Ack掉Panic消息
			//c.AckMessage(conn, ch, msgItem, mcName, "PANIC")
		}

		return
	}()

	//开启channel
	ch, err = c.conn.Channel()
	if err != nil {
		err = errors.New(fmt.Sprintf("failed to open a channel %s", err.Error()))
		c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
		return
	}

	//定义exchange
	err = ch.ExchangeDeclare(exchangeName, string(exchangeType), true, false, false, false, nil)
	if err != nil {
		err = errors.New(fmt.Sprintf("failed to declare exchange %s", err.Error()))
		c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
		return
	}

	//定义队列
	queue, err = ch.QueueDeclare(queueName, false, false, false, false, nil)
	if err != nil {
		err = errors.New(fmt.Sprintf("failed to declare queue %s", err.Error()))
		c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
		return
	}

	//绑定队列
	for _, routeKey := range routes {
		err = ch.QueueBind(queue.Name, routeKey, exchangeName, false, nil)
		if err != nil {
			err = errors.New(fmt.Sprintf("failed to exchange bind queue %s", err.Error()))
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
			return
		}
	}

	//开启消费者
	messages, err = ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		err = errors.New(fmt.Sprintf("failed to consume %s", err.Error()))
		c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] ExceptionX:%s", mcName, id, err.Error()))
		return
	}

	for msg := range messages {
		msgItem = &msg
		c.log.Info(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] [MSG] Message:%s", mcName, id, string(msg.Body)))
		if conn.IsClosed() || ch.IsClosed() {
			return
		}
		for {
			if err = c.proc.Process(msg.Body); err == nil {
				n = 0
				c.AckMessage(conn, ch, &msg, mcName, "")
				break
			} else {
				c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] [PROCESS] [EXCEPTION] Message:%s,  Err:%s", mcName, id, string(msg.Body), err.Error()))
				n++
				if n < c.retryNum {
					c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] [RETRY] [PROCESS] Message:%s, Times:%d", mcName, id, string(msg.Body), n+1))
					continue
				} else {
					n = 0
					c.AckMessage(conn, ch, &msg, mcName, "")
					break
				}
			}
		}
	}
}

func (c *Client) goDelayConsumer(id int, wg *sync.WaitGroup, conn *amqp.Connection, exchangeType ExType, exchangeName string, queueName string, routes []string, mcName string) {
	var (
		err              error
		n                int
		messages         <-chan amqp.Delivery
		msgItem          *amqp.Delivery
		ch               *amqp.Channel
		deadCh           *amqp.Channel
		queue            amqp.Queue
		deadQueue        amqp.Queue
		deadExchangeName = fmt.Sprintf("delay_%s", exchangeName)
		deadQueueName    = fmt.Sprintf("delay_%s", queueName)
	)
	defer wg.Done()
	defer func() {
		if xy := recover(); xy != nil {
			Exception := fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] [PANIC] Msg:%s, Exception:%#v", mcName, id, string(msgItem.Body), xy)
			c.log.Error(Exception)
			//Ack掉Panic消息
			//c.AckMessage(conn, ch, msgItem, mcName, "PANIC")
		}

		return
	}()

	//死信通道
	ch, err = c.conn.Channel()
	if err != nil {
		err = errors.New(fmt.Sprintf("failed to open a channel %s", err.Error()))
		c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
		return
	}
	//死信交换机
	err = ch.ExchangeDeclare(exchangeName, string(exchangeType), true, false, false, false, nil)
	if err != nil {
		err = errors.New(fmt.Sprintf("failed to declare exchange %s", err.Error()))
		c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
		return
	}

	//死信队列
	var args = make(amqp.Table)
	args["x-dead-letter-exchange"] = deadExchangeName
	queue, err = ch.QueueDeclare(queueName, false, false, false, false, args)
	if err != nil {
		err = errors.New(fmt.Sprintf("failed to declare queue %s", err.Error()))
		c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
		return
	}
	//绑定死信队列
	for _, routeKey := range routes {
		err = ch.QueueBind(queue.Name, routeKey, exchangeName, false, nil)
		if err != nil {
			err = errors.New(fmt.Sprintf("failed to exchange bind queue %s", err.Error()))
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
			return
		}
	}

	//业务通道
	deadCh, err = c.conn.Channel()
	if err != nil {
		err = errors.New(fmt.Sprintf("failed to open a channel %s", err.Error()))
		c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
		return
	}
	//业务交换机
	err = deadCh.ExchangeDeclare(deadExchangeName, string(exchangeType), true, false, false, false, nil)
	if err != nil {
		err = errors.New(fmt.Sprintf("failed to declare exchange %s", err.Error()))
		c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
		return
	}

	//死信队列
	deadQueue, err = deadCh.QueueDeclare(deadQueueName, false, false, false, false, nil)

	if err != nil {
		err = errors.New(fmt.Sprintf("failed to declare dead letter queue %s", err.Error()))
		c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
		return
	}
	//绑定死信队列
	err = deadCh.QueueBind(deadQueue.Name, "", deadExchangeName, false, nil)
	if err != nil {
		err = errors.New(fmt.Sprintf("failed to dead letter exchange bind dead letter queue %s", err.Error()))
		c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
		return
	}

	for _, routeKey := range routes {
		err = deadCh.QueueBind(deadQueue.Name, routeKey, deadExchangeName, false, nil)
		if err != nil {
			err = errors.New(fmt.Sprintf("failed to exchange bind queue %s", err.Error()))
			c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s] Exception:%s", mcName, err.Error()))
			return
		}
	}

	//开启消费者
	messages, err = deadCh.Consume(deadQueueName, "", false, false, false, false, nil)
	if err != nil {
		err = errors.New(fmt.Sprintf("failed to consume %s", err.Error()))
		c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] ExceptionX:%s", mcName, id, err.Error()))
		return
	}

	for msg := range messages {
		msgItem = &msg
		c.log.Info(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] [MSG] Message:%s", mcName, id, string(msg.Body)))
		if conn.IsClosed() || ch.IsClosed() {
			return
		}
		for {
			if err = c.proc.Process(msg.Body); err == nil {
				n = 0
				c.AckMessage(conn, ch, &msg, mcName, "")
				break
			} else {
				c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] [PROCESS] [EXCEPTION] Message:%s,  Err:%s", mcName, id, string(msg.Body), err.Error()))
				n++
				if n < c.retryNum {
					c.log.Error(fmt.Sprintf("[MQ] [CONSUMER] [%s][%d] [RETRY] [PROCESS] Message:%s, Times:%d", mcName, id, string(msg.Body), n+1))
					continue
				} else {
					n = 0
					c.AckMessage(conn, ch, &msg, mcName, "")
					break
				}
			}
		}
	}

}
