"""tests/test_redis_client.py — Unit tests for workers/redis_client.py"""
import unittest
from unittest.mock import patch, MagicMock


class TestGetPubsubRedis(unittest.TestCase):
    """get_pubsub_redis() must create a non-singleton client with the correct args."""

    def _call(self, fake_url="redis://localhost:6379/0"):
        with patch("workers.redis_client.REDIS_URL", fake_url), \
             patch("workers.redis_client._redis_lib") as mock_lib:
            mock_lib.from_url.return_value = MagicMock()
            from workers import redis_client
            client = redis_client.get_pubsub_redis()
            return mock_lib.from_url, client

    def test_called_with_redis_url(self):
        url = "redis://testhost:6380/1"
        from_url, _ = self._call(url)
        args, kwargs = from_url.call_args
        self.assertEqual(args[0], url)

    def test_socket_timeout_is_none(self):
        from_url, _ = self._call()
        kwargs = from_url.call_args[1]
        self.assertIn("socket_timeout", kwargs)
        self.assertIsNone(kwargs["socket_timeout"])

    def test_decode_responses_true(self):
        from_url, _ = self._call()
        kwargs = from_url.call_args[1]
        self.assertTrue(kwargs.get("decode_responses"))

    def test_socket_connect_timeout_5(self):
        from_url, _ = self._call()
        kwargs = from_url.call_args[1]
        self.assertEqual(kwargs.get("socket_connect_timeout"), 5)

    def test_returns_new_client_each_call(self):
        """get_pubsub_redis() is not a singleton — returns a fresh client."""
        url = "redis://localhost:6379/0"
        with patch("workers.redis_client.REDIS_URL", url), \
             patch("workers.redis_client._redis_lib") as mock_lib:
            first = MagicMock()
            second = MagicMock()
            mock_lib.from_url.side_effect = [first, second]
            from workers import redis_client
            c1 = redis_client.get_pubsub_redis()
            c2 = redis_client.get_pubsub_redis()
            self.assertIsNot(c1, c2)


class TestGetRedis(unittest.TestCase):
    """get_redis() must use socket_timeout=30 (not None)."""

    def test_socket_timeout_30(self):
        with patch("workers.redis_client._client", None), \
             patch("workers.redis_client.REDIS_URL", "redis://localhost:6379/0"), \
             patch("workers.redis_client._redis_lib") as mock_lib:
            mock_lib.from_url.return_value = MagicMock()
            from workers import redis_client
            redis_client._client = None
            redis_client.get_redis()
            kwargs = mock_lib.from_url.call_args[1]
            self.assertEqual(kwargs.get("socket_timeout"), 30)

    def test_decode_responses_true(self):
        with patch("workers.redis_client._client", None), \
             patch("workers.redis_client.REDIS_URL", "redis://localhost:6379/0"), \
             patch("workers.redis_client._redis_lib") as mock_lib:
            mock_lib.from_url.return_value = MagicMock()
            from workers import redis_client
            redis_client._client = None
            redis_client.get_redis()
            kwargs = mock_lib.from_url.call_args[1]
            self.assertTrue(kwargs.get("decode_responses"))


if __name__ == "__main__":
    unittest.main()
