import logging

import redis


class RedisConnector:
    def __init__(self,
                 host: str,
                 port: int):
        """
        Creates redis connector
        :param host: host of redis
        :param port: port of redis
        """
        self.redis_connection = redis.Redis(host, port)

    def write_items(self, items: list) -> None:
        """
        Writes items to redis
        :param items: list of item
        """
        logging.info("Started writing items")

        for item in items:
            self.redis_connection.sadd(item[0], item[1])

        logging.info("Finished writing items")

    def write_ips(self, ips: list) -> None:
        """
        Writes ips to redis
        :param ips: list of ips
        """
        logging.info("Started writing ips")

        for ip in ips:
            self.redis_connection.sadd(ip[0], ip[1])

        logging.info("Finished writing ips")

    def write_users(self, users: list) -> None:
        """
        Writes users to redis
        :param users: list of users
        """
        logging.info("Started writing users")

        for user in users:
            for device in user[1]:
                self.redis_connection.sadd(user[0], ",".join([device[0], device[1]]))

        logging.info("Finished writing users")

    def write(self,
              users: list,
              ips: list,
              items: list) -> None:
        self.write_users(users)
        self.write_ips(ips)
        self.write_items(items)
