import importlib
import os
import sys
import unittest


class ConfigTests(unittest.TestCase):
    def setUp(self):
        self.env_keys = [
            "TFI_API_KEY",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "AWS_REGION",
        ]
        self.original_values = {key: os.environ.get(key) for key in self.env_keys}
        for key in self.env_keys:
            os.environ.pop(key, None)
        sys.modules.pop("config.config", None)

    def tearDown(self):
        for key, value in self.original_values.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        sys.modules.pop("config.config", None)

    def test_reads_sensitive_values_from_environment(self):
        os.environ["TFI_API_KEY"] = "env-tfi-key"
        os.environ["AWS_ACCESS_KEY_ID"] = "env-access-key"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "env-secret-key"
        os.environ["AWS_SESSION_TOKEN"] = "env-session-token"

        module = importlib.import_module("config.config")

        self.assertEqual(module.TFI_API_KEY, "env-tfi-key")
        self.assertEqual(module.AWS_ACCESS_KEY_ID, "env-access-key")
        self.assertEqual(module.AWS_SECRET_ACCESS_KEY, "env-secret-key")
        self.assertEqual(module.AWS_SESSION_TOKEN, "env-session-token")

    def test_uses_defaults_when_env_missing(self):
        module = importlib.import_module("config.config")

        self.assertEqual(module.AWS_REGION, "us-east-1")
        self.assertEqual(module.KINESIS_STREAM_NAME, "dublin-bus-stream")


if __name__ == "__main__":
    unittest.main()
