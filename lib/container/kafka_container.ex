# SPDX-License-Identifier: MIT
defmodule Testcontainers.KafkaContainer do
  @moduledoc """
  Provides functionality for creating and managing Kafka container configurations.
  """
  alias Testcontainers.Container

  alias Testcontainers.Container
  alias Testcontainers.KafkaContainer
  alias Testcontainers.LogWaitStrategy
  alias Testcontainers.PortWaitStrategy

  @default_image_with_tag "confluentinc/cp-kafka:7.4.3"
  @default_wait_timeout 60_000

  @enforce_keys [:image, :wait_timeout]
  defstruct [:image, :wait_timeout]

  @doc """
  Creates a new `KafkaContainer` struct with default configurations.
  """
  def new do
    %__MODULE__{
      image: @default_image_with_tag,
      wait_timeout: @default_wait_timeout
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
  Overrides the default wait timeout used for the Kafka container.
  """
  def with_wait_timeout(%__MODULE__{} = config, wait_timeout) when is_integer(wait_timeout) do
    %{config | wait_timeout: wait_timeout}
  end

  # TODO We must alter kafka after start to set proper broker urls, with exposed ports
  _ = """
    private async updateAdvertisedListeners(container: StartedTestContainer, inspectResult: InspectResult) {
      const brokerAdvertisedListener = `BROKER://${inspectResult.hostname}:${KAFKA_BROKER_PORT}`;

      let bootstrapServers = `PLAINTEXT://${container.getHost()}:${container.getMappedPort(KAFKA_PORT)}`;
      if (this.saslSslConfig) {
        if (this.networkMode) {
          bootstrapServers = `${bootstrapServers},SECURE://${inspectResult.hostname}:${this.saslSslConfig.port}`;
        } else {
          bootstrapServers = `${bootstrapServers},SECURE://${container.getHost()}:${container.getMappedPort(
            this.saslSslConfig.port
          )}`;
        }
      }

      const { output, exitCode } = await container.exec([
        "kafka-configs",
        "--alter",
        "--bootstrap-server",
        brokerAdvertisedListener,
        "--entity-type",
        "brokers",
        "--entity-name",
        this.environment["KAFKA_BROKER_ID"],
        "--add-config",
        `advertised.listeners=[${bootstrapServers},${brokerAdvertisedListener}]`,
      ]);

      if (exitCode !== 0) {
        throw new Error(`Kafka container configuration failed with exit code ${exitCode}: ${output}`);
      }
    }
  """

  defimpl Testcontainers.ContainerBuilder do
    import Container

    @spec build(%KafkaContainer{}) :: %Container{}
    @impl true
    def build(%KafkaContainer{} = config) do
      new(config.image)
      |> with_exposed_port(9092)
      |> with_environment(:KAFKA_NODE_ID, "1")
      |> with_environment(:KAFKA_CONTROLLER_LISTENER_NAMES, "CONTROLLER")
      |> with_environment(
        :KAFKA_LISTENER_SECURITY_PROTOCOL_MAP,
        "CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT"
      )
      |> with_environment(
        :KAFKA_LISTENERS,
        "INTERNAL://0.0.0.0:29092,CONTROLLER://0.0.0.0:29093,EXTERNAL://0.0.0.0:9092"
      )
      |> with_environment(
        :KAFKA_ADVERTISED_LISTENERS,
        "INTERNAL://localhost:29092,EXTERNAL://#{Testcontainers.get_host()}:9092"
      )
      |> with_environment(:KAFKA_INTER_BROKER_LISTENER_NAME, "INTERNAL")
      |> with_environment(:KAFKA_CONTROLLER_QUORUM_VOTERS, "1@#{Testcontainers.get_host()}:29093")
      |> with_environment(:KAFKA_PROCESS_ROLES, "broker,controller")
      |> with_environment(:KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS, "0")
      |> with_environment(:KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, "1")
      |> with_environment(:CLUSTER_ID, "ciWo7IWazngRchmPES6q5A==")
      |> with_environment(:KAFKA_LOG_DIRS, "/tmp/kraft-combined-logs")
      |> with_waiting_strategies([
        LogWaitStrategy.new(~r/.*Kafka Server started.*/, config.wait_timeout),
        PortWaitStrategy.new(Testcontainers.get_host(), 9092, config.wait_timeout)
      ])
    end
  end
end
