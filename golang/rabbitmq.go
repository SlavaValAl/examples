func (conn amqp.Connection, channel amqp.Channel, msgs chan Message) Listen() {
	var (
		amqpErr       *amqp.Error
		amqpConnError = make(chan *amqp.Error)
		amqpReturn    = make(chan amqp.Return)
	)

	conn.NotifyClose(amqpConnError)
	channel.NotifyReturn(amqpReturn)

sendLoop:
	for {
		select {
		case <-s.done:
			if err := shutdown(s.conn, s.channel, s.tag); err != nil {
				l(log).Errorf("failed to shutdown amqp connection. err: %s", err)
			}
			break sendLoop
		case ret := <-amqpReturn:
			l(log).Fatalf("producer publish returned. code: %d, text: %s, body: %s",
				ret.ReplyCode, ret.ReplyText, ret.Body)
		case amqpErr = <-amqpConnError:
			if amqpErr != nil && amqpErr.Recover {
				continue
			}
			l(log).Warnf("Connection to AMQP lost: %s. Reconnecting...", amqpErr)
			amqpConnError = make(chan *amqp.Error)
			channel.NotifyClose(amqpConnError)
		case msg := <-msgs:
			if err := publish(channel, msg.Payload, msg.Cluster); err != nil {
				l(log).Fatalf("producer publish failed: %s", err)
			}
		}
	}
}

func publish(channel Channel, body []byte, cluster string) error {
	l(log).Debugf("amqp publishing %dB body (%s)", len(body), body)
	if err := channel.Publish(
		exchangeName(),         // publish to an exchange
		cluster,                // routing to 0 or more queues
		true,                   // mandatory
		false,                  // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "application/json",
			ContentEncoding: "",
			Body:            body,
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0,              // 0-9
			// a bunch of application/implementation-specific fields
		},
	); err != nil {
		return fmt.Errorf("exchange publish error: %s", err)
	}
	return nil
}

func shutdown(conn amqp.Connection, channel amqp.Channel, tag string) error {
	defer l(log).Info("AMQP shutdown OK")

	if err := channel.Cancel(tag, true); err != nil {
		return fmt.Errorf("consumer cancel failed: %s", err)
	}

	if err := conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	return nil
}

func connect(uri string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(uri)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

