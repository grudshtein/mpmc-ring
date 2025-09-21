#include "mpmc.hpp"

#include <gtest/gtest.h>
#include <memory>
#include <stdexcept>
#include <vector>

TEST(MpmcRing, Construct) {
  const int capacity = 8;
  mpmc::MpmcRing<int> ring(capacity);
  EXPECT_EQ(ring.capacity(), capacity);
  EXPECT_EQ(ring.size(), 0);
  EXPECT_TRUE(ring.empty());
  EXPECT_FALSE(ring.full());
}

TEST(MpmcRing, Capacity) {
  // valid capacities
  EXPECT_NO_THROW(mpmc::MpmcRing<int> ring(2));
  EXPECT_NO_THROW(mpmc::MpmcRing<int> ring(16));
  EXPECT_NO_THROW(mpmc::MpmcRing<int> ring(65'536));

  // invalid capacities
  EXPECT_THROW(mpmc::MpmcRing<int> ring(1), std::invalid_argument);  // < 2
  EXPECT_THROW(mpmc::MpmcRing<int> ring(18), std::invalid_argument); // not power-of-two
}

TEST(MpmcRing, DISABLED_BasicPushPop) {
  const auto capacity = 8;
  mpmc::MpmcRing<int> ring(capacity);

  // test basic push
  for (auto i = 0; i != capacity; ++i) {
    ASSERT_TRUE(ring.try_push(i * i));
  }

  // test basic pop
  int v;
  for (auto i = 0; i != capacity; ++i) {
    ASSERT_TRUE(ring.try_pop(v));
    EXPECT_EQ(v, i * i);
  }
}

TEST(MpmcRing, DISABLED_FullEmptyBoundaries) {
  const auto capacity = 8;
  mpmc::MpmcRing<int> ring(capacity);

  // test push boundaries
  for (auto i = 0; i != capacity; ++i) {
    EXPECT_FALSE(ring.full());
    ASSERT_TRUE(ring.try_push(i * i));
  }
  EXPECT_TRUE(ring.full());
  EXPECT_FALSE(ring.try_push(999));

  // test pop boundaries
  int v;
  for (auto i = 0; i != capacity; ++i) {
    ASSERT_TRUE(ring.try_pop(v));
    EXPECT_FALSE(ring.full());
  }
  EXPECT_TRUE(ring.empty());
  EXPECT_FALSE(ring.try_pop(v));
}

TEST(MpmcRing, DISABLED_WrapAroundFifo) {
  const auto capacity = 8;
  mpmc::MpmcRing<int> ring(capacity);

  // fill: 0..7
  for (auto i = 0; i != capacity; ++i) {
    ASSERT_TRUE(ring.try_push(i));
  }
  EXPECT_TRUE(ring.full());
  EXPECT_FALSE(ring.try_push(999));

  // pop half: 0..3
  int v;
  for (auto i = 0; i != capacity / 2; ++i) {
    ASSERT_TRUE(ring.try_pop(v));
  }
  EXPECT_FALSE(ring.full());

  // refill: 8..11 (forces wrap)
  for (auto i = 0; i != capacity / 2; ++i) {
    ASSERT_TRUE(ring.try_push(capacity + i));
  }
  EXPECT_TRUE(ring.full());
  EXPECT_FALSE(ring.try_push(999));

  // drain: 4..11 (FIFO across wrap)
  for (auto i = 0; i != capacity; ++i) {
    ASSERT_TRUE(ring.try_pop(v));
    EXPECT_EQ(v, i + capacity / 2);
  }
  EXPECT_TRUE(ring.empty());
}

TEST(MpmcRing, DISABLED_MoveOnlyType) {
  const auto capacity = 8;
  mpmc::MpmcRing<std::unique_ptr<int>> ring(capacity);

  // initialize values
  std::vector<std::unique_ptr<int>> vals;
  vals.reserve(capacity);
  for (auto i = 0; i != capacity; ++i) {
    vals.emplace_back(std::make_unique<int>(i));
  }

  // push by move
  for (auto i = 0; i != capacity; ++i) {
    ASSERT_TRUE(ring.try_push(std::move(vals[i])));
    EXPECT_EQ(vals[i], nullptr); // source becomes null
  }
  EXPECT_TRUE(ring.full());

  // pop and verify FIFO
  std::unique_ptr<int> out;
  for (auto i = 0; i != capacity; ++i) {
    ASSERT_TRUE(ring.try_pop(out));
    ASSERT_NE(out, nullptr);
    EXPECT_EQ(*out, i);
  }
  EXPECT_TRUE(ring.empty());
}