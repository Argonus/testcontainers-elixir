defmodule Testcontainers.Container.KafkaContainerTest do
  use ExUnit.Case, async: true
  import Testcontainers.ExUnit

  alias Testcontainers.Container
  alias Testcontainers.KafkaContainer

  describe "new/0" do
    test "creates a new KafkaContainer struct with default configurations" do
      config = KafkaContainer.new()

      assert config.image == "confluentinc/cp-kafka:7.4.3"
      assert config.wait_timeout == 60_000
    end
  end

  describe "with_image/2" do
    test "overrides the default image used for the Kafka container" do
      config = KafkaContainer.new()
      new_config = KafkaContainer.with_image(config, "confluentinc/cp-kafka:6.2.0")

      assert new_config.image == "confluentinc/cp-kafka:6.2.0"
    end

    test "raises if the image is not a binary" do
      config = KafkaContainer.new()
      assert_raise FunctionClauseError, fn -> KafkaContainer.with_image(config, 6.2) end
    end
  end

  describe "with_wait_timeout/2" do
    test "overrides the default wait timeout used for the Kafka container" do
      config = KafkaContainer.new()
      new_config = KafkaContainer.with_wait_timeout(config, 60_001)

      assert new_config.wait_timeout == 60_001
    end

    test "raises if the wait timeout is not an integer" do
      config = KafkaContainer.new()

      assert_raise FunctionClauseError, fn ->
        KafkaContainer.with_wait_timeout(config, "60_001")
      end
    end
  end

  describe "integration testing" do
    container(
      :kafka,
      KafkaContainer.with_image(KafkaContainer.new(), "confluentinc/cp-kafka:latest.arm64")
    )

    test "provides a ready-to-use kafka container", %{kafka: kafka} do
      uris = [{Testcontainers.get_host(), Container.mapped_port(kafka, 9092)}] |> IO.inspect()

      {:ok, pid} = KafkaEx.create_worker(:test_worker, uris: uris, consumer_group: "kafka_ex")
      on_exit(pid, fn -> KafkaEx.stop_worker(:worker) end)

      request = %KafkaEx.Protocol.CreateTopics.TopicRequest{
        topic: "test_topic",
        num_partitions: 1,
        replication_factor: 1,
        replica_assignment: []
      }

      _ = KafkaEx.create_topics([request], worker_name: :test_worker)

      {:ok, _} =
        KafkaEx.produce("test_topic", 0, "hey", worker_name: :test_worker, required_acks: 1)

      stream = KafkaEx.stream("test_topic", 0, worker_name: :test_worker)
      [response] = Enum.take(stream, 1)

      assert response.value == "hey"
    end
  end
end
