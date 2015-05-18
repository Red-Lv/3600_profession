__author__ = 'lvleibing'

import zmq


class StreamerDevice(object):

    def __init__(self):

        self.context = zmq.Context()

        self.frontend = self.context.socket(zmq.PULL)
        self.backend = self.context.socket(zmq.PUSH)

        self.frontend_port = 4160
        self.backend_port = 4161

    def init(self, *args, **kwargs):

        self.frontend_port = kwargs.get('frontend_port', self.frontend_port)
        self.backend_port = kwargs.get('backend_port', self.backend_port)

        return True

    def run(self):

        try:
            self.frontend.bind('tcp://*:{0}'.format(self.frontend_port))
            self.backend.bind('tcp://*:{0}'.format(self.backend_port))

            zmq.device(zmq.STREAMER, self.frontend, self.backend)
        except Exception as e:
            print 'streamer device running error. err: {0}'.format(e)
        finally:
            self.frontend.close()
            self.backend.close()
            self.context.term()

        return True


class StreamerProducer(object):

    def __init__(self):

        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUSH)

        self.streamer_addr = None

    def init(self, streamer_addr):

        self.streamer_addr = streamer_addr

        return True

    def run(self):

        try:
            self.socket.connect(self.streamer_addr)
        except Exception as e:
            print 'fail to connect to streamer device. err: {0}'.format(e)

        return True

    def produce(self, data):
        """

        :param data: json
        :return:
        """

        try:
            self.socket.send_json(data)
        except Exception as e:
            print 'fail to send data. err: {0}'.format(e)

        return True


class StreamerConsumer(object):

    def __init__(self):

        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PULL)

        self.streamer_addr = None

    def init(self, streamer_addr):

        self.streamer_addr = streamer_addr

        return True

    def run(self):

        try:
            self.socket.connect(self.streamer_addr)
        except Exception as e:
            print 'fail to connect to streamer device. err: {0}'.format(e)

        return True

    def consume(self):
        """

        :return:
        """

        while True:

            data = self.socket.recv_json()
            yield data

if __name__ == '__main__':

    # streamer_device
    streamer_device = StreamerDevice()
    streamer_device.init()
    streamer_device.run()

    # streamer_producer
    streamer_producer = StreamerProducer()
    streamer_producer.init('tcp://127.0.0.1:4160')
    streamer_producer.run()
    streamer_producer.produce({'name': 'Red Lv'})

    # streamer_consumer
    streamer_consumer = StreamerConsumer()
    streamer_consumer.init('tcp://127.0.0.1:4161')
    streamer_consumer.run()
    for data in streamer_consumer.consume():
        print data
