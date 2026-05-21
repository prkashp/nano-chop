"""Snowflake connection management with key-pair authentication and connection pooling."""

import os
import queue
import threading
from typing import Optional
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import load_pem_private_key
import snowflake.connector


def load_private_key(key_path: str, passphrase: Optional[str] = None) -> bytes:
    """
    Load PEM-formatted private key and return DER bytes.

    Args:
        key_path: Path to private key file (PEM format)
        passphrase: Optional passphrase for encrypted key

    Returns:
        Private key in DER format (bytes)

    Raises:
        FileNotFoundError: If key file not found
        ValueError: If key format is invalid
    """
    key_path = os.path.expandvars(os.path.expanduser(key_path))

    if not os.path.exists(key_path):
        raise FileNotFoundError(f"Private key not found at {key_path}")

    with open(key_path, 'rb') as f:
        pem_bytes = f.read()

    password = None
    if passphrase:
        passphrase = os.path.expandvars(os.path.expanduser(passphrase))
        if passphrase:
            password = passphrase.encode('utf-8')

    try:
        private_key = load_pem_private_key(
            pem_bytes,
            password=password,
            backend=default_backend()
        )
        return private_key.private_bytes(
            encoding=__import__('cryptography.hazmat.primitives.serialization',
                              fromlist=['Encoding']).Encoding.DER,
            format=__import__('cryptography.hazmat.primitives.serialization',
                            fromlist=['PrivateFormat']).PrivateFormat.PKCS8,
            encryption_algorithm=__import__('cryptography.hazmat.primitives.serialization',
                                          fromlist=['NoEncryption']).NoEncryption()
        )
    except Exception as e:
        raise ValueError(f"Failed to load private key: {e}")


def get_connection(config: dict) -> snowflake.connector.SnowflakeConnection:
    """
    Create a new Snowflake connection with key-pair authentication.

    Args:
        config: Dict with keys: account, user, warehouse, database, role,
                private_key_path, private_key_passphrase

    Returns:
        SnowflakeConnection instance

    Raises:
        snowflake.connector.Error: If connection fails
        ValueError: If config is incomplete or key cannot be loaded
    """
    required = ['account', 'user', 'warehouse', 'database', 'private_key_path']
    missing = [k for k in required if k not in config]
    if missing:
        raise ValueError(f"Missing required config keys: {missing}")

    private_key_bytes = load_private_key(
        config['private_key_path'],
        config.get('private_key_passphrase')
    )

    conn = snowflake.connector.connect(
        account=config['account'],
        user=config['user'],
        private_key=private_key_bytes,
        warehouse=config['warehouse'],
        database=config['database'],
        role=config.get('role', 'SYSADMIN')
    )

    return conn


class ConnectionPool:
    """Thread-safe connection pool for Snowflake."""

    def __init__(self, config: dict, pool_size: int = 5):
        """
        Initialize connection pool.

        Args:
            config: Snowflake connection config dict
            pool_size: Number of connections to pre-create
        """
        self.config = config
        self.pool_size = pool_size
        self._pool: queue.Queue = queue.Queue(maxsize=pool_size)
        self._lock = threading.Lock()
        self._initialized = False

    def _initialize(self):
        """Lazily initialize connections."""
        if self._initialized:
            return

        with self._lock:
            if self._initialized:
                return

            for _ in range(self.pool_size):
                try:
                    conn = get_connection(self.config)
                    self._pool.put(conn)
                except Exception as e:
                    raise RuntimeError(f"Failed to initialize connection pool: {e}")

            self._initialized = True

    def acquire(self) -> snowflake.connector.SnowflakeConnection:
        """
        Acquire a connection from the pool, creating one if needed.

        Returns:
            SnowflakeConnection instance
        """
        self._initialize()

        try:
            conn = self._pool.get_nowait()
        except queue.Empty:
            conn = get_connection(self.config)

        return conn

    def release(self, conn: snowflake.connector.SnowflakeConnection):
        """
        Return a connection to the pool.

        Args:
            conn: SnowflakeConnection to return
        """
        if conn is None:
            return

        try:
            self._pool.put_nowait(conn)
        except queue.Full:
            conn.close()

    def close_all(self):
        """Close all connections in the pool."""
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except queue.Empty:
                break

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close_all()
