defmodule Testcontainers.KafkaContainer do
  @moduledoc """
  Provides functionality for creating and managing Kafka container configurations.
  """
  alias Testcontainers.Container

  alias Testcontainers.Container
  alias Testcontainers.KafkaContainer
  alias Testcontainers.CommandWaitStrategy

  @default_image "confluentinc/cp-kafka"
  @default_image_with_tag "confluentinc/cp-kafka:7.4.3"
  @default_kafka_port 9092
  @default_broker_port 29092
  @default_zookeeper_port 2181
  @default_wait_timeout 60_000
  @default_zookeeper_strategy :embedded
  @default_topic_partitions 1
  @default_kraft_enabled false

  @enforce_keys [
    :image,
    :kafka_port,
    :broker_port,
    :zookeeper_port,
    :wait_timeout,
    :zookeeper_strategy,
    :default_topic_partitions,
    :kraft_enabled
  ]
  defstruct [
    :image,
    :kafka_port,
    :broker_port,
    :zookeeper_port,
    :wait_timeout,
    :zookeeper_strategy,
    :default_topic_partitions,
    :kraft_enabled
  ]

  @doc """
  Creates a new `KafkaContainer` struct with default configurations.
  """
  def new do
    %__MODULE__{
      image: @default_image_with_tag,
      kafka_port: @default_kafka_port,
      broker_port: @default_broker_port,
      zookeeper_port: @default_zookeeper_port,
      wait_timeout: @default_wait_timeout,
      zookeeper_strategy: @default_zookeeper_strategy,
      default_topic_partitions: @default_topic_partitions,
      kraft_enabled: @default_kraft_enabled
    }
  end

  @doc """
  Overrides the default image used for the Kafka container.
  Right now we support only confluentinc images.
  """
  def with_image(%__MODULE__{} = config, image) when is_binary(image) do
    %{config | image: image}
  end

  @doc """
  Overrides the default kafka port used for the Kafka container.
  """
  def with_kafka_port(%__MODULE__{} = config, kafka_port) when is_integer(kafka_port) do
    %{config | kafka_port: kafka_port}
  end

  @doc """
  Overrides the default kafka port used for the Kafka container.
  """
  def with_broker_port(%__MODULE__{} = config, broker_port) when is_integer(broker_port) do
    %{config | broker_port: broker_port}
  end

  @doc """
  Overrides the default zookeeper port used for the Kafka container.
  """
  def with_zookeeper_port(%__MODULE__{} = config, zookeeper_port)
      when is_integer(zookeeper_port) do
    %{config | zookeeper_port: zookeeper_port}
  end

  @doc """
  Overrides the default zookeeper strategy used for the Kafka container.
  """
  def with_zookeeper_strategy(%__MODULE__{} = config, zookeeper_strategy)
      when zookeeper_strategy in [:embedded, :external] do
    %{config | zookeeper_strategy: zookeeper_strategy}
  end

  @doc """
  Overrides the default kraft enabled used for the Kafka container.
  """
  def with_kraft_enabled(%__MODULE__{} = config, kraft_enabled) when is_boolean(kraft_enabled) do
    %{config | kraft_enabled: kraft_enabled}
  end

  @doc """
  Overrides the default wait timeout used for the Kafka container.
  """
  def with_wait_timeout(%__MODULE__{} = config, wait_timeout) when is_integer(wait_timeout) do
    %{config | wait_timeout: wait_timeout}
  end

  @doc """
  Overrides the default topic
  """
  def with_topic_partitions(%__MODULE__{} = config, topic_partitions)
      when is_integer(topic_partitions) do
    %{config | default_topic_partitions: topic_partitions}
  end

  defimpl Testcontainers.ContainerBuilder do
    import Container

    @spec build(%KafkaContainer{}) :: %Container{}
    @impl true
    def build(%KafkaContainer{} = config) do
      new(config.image)
      |> with_exposed_port(config.kafka_port)
      |> with_environment(:KAFKA_BROKER_ID, "1")
      |> with_listener_config(config)
      |> with_topic_config(config)
      |> maybe_with_zookeeper(config)
      |> maybe_with_kraft(config)
    end

    # ------------------Listeners------------------
    defp with_listener_config(container, config) do
      container
      |> with_environment(
        :KAFKA_LISTENERS,
        "PLAINTEXT://0.0.0.0:#{config.kafka_port},BROKER://0.0.0.0:9092"
      )
      |> with_environment(
        :KAFKA_LISTENER_SECURITY_PROTOCOL_MAP,
        "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
      )
      |> with_environment(:KAFKA_INTER_BROKER_LISTENER_NAME, "BROKER")
    end

    defp with_topic_config(container, config) do
      container
      |> with_environment(:KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, "1")
      |> with_environment(:KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS, "1")
      |> with_environment(:KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR, "1")
      |> with_environment(:KAFKA_TRANSACTION_STATE_LOG_MIN_ISR, "1")
    end

    # ------------------Zookeeper------------------
    defp maybe_with_zookeeper(container, config = %{kraft_enabled: false}) do
    end

    defp maybe_with_zookeeper(container, _config), do: container

    # ------------------Kraft------------------
    defp maybe_with_kraft(container, config = %{kraft_enabled: true}) do
    end

    defp maybe_with_kraft(container, _), do: container
  end
end
