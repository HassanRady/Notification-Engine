import logging
import math
import hashlib
from typing import List, Optional, Callable, Awaitable
from redis.asyncio import Redis
from app.config import settings

logger = logging.getLogger(__name__)

class BloomFilter:
    """
    Pure Python, zero-dependency in-memory Bloom Filter to prevent unnecessary database/cache lookups.
    Optimizes lookups by filtering out elements that are definitively NOT present.
    """
    def __init__(self, expected_elements: int = 100000, false_positive_rate: float = 0.01) -> None:
        self.expected_elements = expected_elements
        self.false_positive_rate = false_positive_rate
        
        # Calculate optimal size (m) and hash function count (k)
        self.size = self._get_size(expected_elements, false_positive_rate)
        self.hash_count = self._get_hash_count(self.size, expected_elements)
        self.bit_array = [False] * self.size
        logger.info(
            f"BloomFilter configured: bits={self.size}, hash_functions={self.hash_count}, "
            f"expected_elements={expected_elements}, target_fpr={false_positive_rate}"
        )

    def _get_size(self, n: int, p: float) -> int:
        """Calculates optimal number of bits (m) for the bit array."""
        m = -(n * math.log(p)) / (math.log(2) ** 2)
        return max(1, int(m))

    def _get_hash_count(self, m: int, n: int) -> int:
        """Calculates optimal number of hash functions (k)."""
        k = (m / n) * math.log(2)
        return max(1, int(k))

    def _hashes(self, item: str) -> List[int]:
        """Generates k different bit indices using unique salts with hashlib.md5."""
        indices = []
        item_bytes = item.encode("utf-8")
        for i in range(self.hash_count):
            salt = str(i).encode("utf-8")
            hash_val = hashlib.md5(item_bytes + salt).hexdigest()
            index = int(hash_val, 16) % self.size
            indices.append(index)
        return indices

    def add(self, item: str) -> None:
        """Adds an item to the Bloom Filter bit array."""
        for index in self._hashes(item):
            self.bit_array[index] = True

    def contains(self, item: str) -> bool:
        """
        Tests membership of an item in the Bloom Filter.
        
        Returns:
            False: Definitive No (the element has not been added).
            True: Probabilistic Yes (the element has likely been added, with FPR probability).
        """
        for index in self._hashes(item):
            if not self.bit_array[index]:
                return False
        return True

# Singleton DND Bloom Filter instance.
# Used globally by the API layer to track users with custom settings.
dnd_bloom_filter = BloomFilter()

async def check_user_dnd_status(
    user_id: str,
    redis_client: Redis,
    db_fallback_query: Callable[[str], Awaitable[Optional[bool]]]
) -> bool:
    """
    Asynchronously checks if a user has custom DND settings configured.
    
    Logic Flow:
      1. Check in-memory Bloom Filter first.
      2. If False (Definitive No): Skip Redis/DB check entirely, return False (Inactive DND).
      3. If True (Probabilistic Yes): Fallback to Cache-Aside strategy:
         a. Lookup the DND setting in Redis.
         b. If cached, parse and return the cached status.
         c. If not cached, invoke db_fallback_query to retrieve from the Database.
         d. Write the resolved database value to Redis cache (with a 3600s TTL) and update the Bloom Filter.
         
    Args:
        user_id: The recipient user identifier.
        redis_client: Active redis.asyncio client connection.
        db_fallback_query: Async callable querying the DB for DND settings (returns True if DND is active, False/None if not).
        
    Returns:
        bool: True if DND is active, False if DND is inactive (system default).
    """
    # 1. Bloom Filter Check
    if not dnd_bloom_filter.contains(user_id):
        logger.debug(f"Bloom filter definitive exclusion hit for user: {user_id}. Skipping Redis/DB checks.")
        return False  # Inactive DND (System default)

    logger.debug(f"Bloom filter probabilistic collision hit for user: {user_id}. Querying cache...")

    redis_key = f"{settings.dnd_redis_prefix}{user_id}"
    
    # 2. Redis Lookup (Cache-Aside Step 1)
    try:
        cached_val = await redis_client.get(redis_key)
        if cached_val is not None:
            logger.debug(f"Redis cache hit for user: {user_id} DND = {cached_val}")
            return cached_val.lower() == "true"
    except Exception as e:
        logger.warning(f"Error reading DND preference from Redis cache for user {user_id}: {e}", exc_info=True)

    # 3. Database Lookup (Cache-Aside Step 2)
    logger.info(f"Cache miss for user: {user_id}. Querying Database...")
    db_val = None
    try:
        db_val = await db_fallback_query(user_id)
    except Exception as e:
        logger.error(f"Failed to query database DND settings for user {user_id}: {e}", exc_info=True)

    # Resolve settings (DND is active only if database record is True)
    dnd_active = bool(db_val)

    # Cache the resolved result back to Redis
    try:
        await redis_client.set(redis_key, str(dnd_active).lower(), ex=3600)
        logger.info(f"Cached DND status ({dnd_active}) in Redis for user: {user_id}")
    except Exception as e:
        logger.warning(f"Failed to write DND status to Redis for user {user_id}: {e}", exc_info=True)

    # Seed the Bloom Filter if database lookup confirms a custom config exists
    if db_val is not None:
        dnd_bloom_filter.add(user_id)

    return dnd_active
