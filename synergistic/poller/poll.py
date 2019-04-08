import sys
import select
import socket
import typing
import threading


class EventNotImplementedError(Exception):
    """
    exception for when an object is missing a method
    """
    pass


class NotAnError(Exception):
    """
    mock exception that for disabling error handling
    """
    pass


class Poll:

    def __init__(self, poll=None, catch_errors: bool=True):
        """
        create new poller
        :param poll: existing poll object
        :param catch_errors: should all exceptions be caught
        """
        if poll is None:
            if sys.platform == 'win32':
                poll = select.select()
            else:
                poll = select.epoll()
        if isinstance(poll, select.epoll):
            self.object = 'epoll'
        elif isinstance(poll, select.poll):
            self.object = 'poll'
        elif isinstance(poll, select.select):
            self.object = 'select'

        self.poll = poll
        self.open = True
        self.workers = []
        self.servers = {}
        self.clients = {}
        self.events = []
        self.worker_count = 1
        self.catch = NotAnError
        if catch_errors:
            self.catch = Exception

    def add_server(self, server_conn: typing.Type[socket.socket]=None, server_fd: int=None):
        """
        add a socket object to the poller as a server
        server connection must implement on_event that returns a client
        :param server_conn: socket object
        :param server_fd: socket's file descriptor
        :return: None
        """
        if not hasattr(server_conn, 'on_connect'):
            raise EventNotImplementedError("on_connect method has not been implemented")
        if server_fd is None:
            server_fd = server_conn.fileno()

        self.servers[server_fd] = server_conn
        self._add(server_conn)

    def add_client(self, client_conn: typing.Type[socket.socket], client_fd: int=None):
        """
        add a socket object to the poller as a client
        :param client_conn: socket object
        :param client_fd: socket's file descriptor
        :return: None
        """
        if not hasattr(client_conn, 'on_receive'):
            raise EventNotImplementedError("on_receive method has not been implemented")
        #if not hasattr(client_conn, 'on_send'):
        #    raise EventNotImplementedError("on_send method has not been implemented")
        if client_fd is None:
            client_fd = client_conn.fileno()
        if hasattr(client_conn, 'on_connect'):
            client_conn.on_connect()
        self.clients[client_fd] = client_conn
        self._add(client_conn)

    def _add(self, conn):
        if self.object == 'epoll' or self.object == 'poll':
            # select.POLLIN == select.EPOLLIN
            self.poll.register(conn.fileno(), select.POLLIN)
        elif self.object == 'select':
            new_list = list(self.servers.values()) + list(self.clients.values())
            self.poll = select.select(new_list, [], [])

    def remove(self, conn: typing.Type[socket.socket]=None, fd: int=None):
        """
        remove a socket object from the poller using either its object or file descriptor
        :param conn: socket object to remove
        :param fd: file descriptor to remove
        :return: None
        """
        if not conn and fd is None:
            raise AttributeError
        if fd is None:
            fd = conn.fileno()

        if fd in self.clients:
            del self.clients[fd]
        else:
            if fd in self.servers:
                del self.servers[fd]

        if self.object == 'epoll' or self.object == 'poll':
            try:
                self.poll.unregister(fd)
            except KeyError:
                pass
        elif self.object == 'select':
            new_list = list(self.servers.values()) + list(self.clients.values())
            self.poll = select.select(new_list, [], [])

    def server_connect_event(self, server_fd: int=None, server_conn: typing.Type[socket.socket]=None):
        """
        handle an event for a server object
        :param server_fd: file descriptor of the server
        :param server_conn: socket object of the server
        :return: None
        """
        if not server_conn:
            if server_fd is None:
                raise AttributeError
            server_conn = self.servers[server_fd]

        try:
            client_conn = server_conn.on_connect()
            if client_conn:
                self.add_client(client_conn)
        except self.catch as e:
            print("Error while handling a server event for", server_fd, ":", e)

    def client_receive_event(self, client_fd: int=None, client_conn: typing.Type[socket.socket]=None):
        """
        handle an event for a client object
        :param client_fd: file descriptor of the client
        :param client_conn: socket object of the client
        :return:
        """
        if not client_conn:
            if client_fd is None:
                raise AttributeError
            client_conn = self.clients[client_fd]

        if hasattr(client_conn, 'on_receive'):
            try:
                client_conn.on_receive()
            except self.catch as e:
                client_conn.close()
                print("Error while handling a client event from", client_fd, ":", e)

    def client_send_event(self, client_fd: int=None, client_conn: typing.Type[socket.socket]=None):
        """
        handle an event for a client object
        :param client_fd: file descriptor of the client
        :param client_conn: socket object of the client
        :return:
        """
        if not client_conn:
            if client_fd is None:
                raise AttributeError
            client_conn = self.clients[client_fd]

        if hasattr(client_conn, 'on_send'):
            try:
                client_conn.on_send()
            except self.catch as e:
                client_conn.close()
                print("Error while handling a client event from", client_fd, ":", e)

    def serve_once(self, worker_id: int=0):
        """
        iterates through all the jobs that need doing and executing them
        :param worker_id: id of worker it is acting as
        :return: None
        """
        if not worker_id:
            events = self.poll.poll()
            self.events = [events[i::self.worker_count] for i in range(self.worker_count)]

        for fd, event in self.events[worker_id]:
            if event == select.POLLIN:
                if fd in self.clients:
                    self.client_receive_event(fd)
                elif fd in self.servers:
                    self.server_connect_event(fd)
                else:
                    print("Error could not find ", fd, "doing", event, "on worker", worker_id)

        if not worker_id:
            for fd, conn in {**self.clients, **self.servers}.items():
                if conn._closed:
                    if hasattr(conn, 'on_disconnect'):
                        conn.on_disconnect()
                    self.remove(conn, fd)

    def worker(self, worker_id: int=0):
        """
        create a worker (blocking) that iterates through jobs that need doing and executing them
        :param worker_id: id of worker being ran
        :return: None
        """
        while self.open:
            self.serve_once(worker_id)

    def set_worker_count(self, worker_count: int=1):
        """
        set the number of workers to use
        :param worker_count: number of workers to use
        :return: None
        """
        self.worker_count = worker_count

    def serve_forever(self):
        """
        start the poller
        :return: None
        """
        for worker_id in range(1, self.worker_count):
            worker = threading.Thread(target=self.worker, args=(worker_id,), daemon=True)
            self.workers.append(worker)
        self.worker(0)
        for worker in self.workers:
            worker.start()

    def stop(self):
        """
        stop the poller
        :return: None
        """
        self.open = False
