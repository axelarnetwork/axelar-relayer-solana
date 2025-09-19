# Solana Relayer



Relayer has four primary components:

- **Subscriber** – Reads relevant transactions from Solana and publishes tasks to RabbitMQ.
- **Distributor** – Reads upcoming tasks from the GMP API and publishes them to RabbitMQ.
- **Ingestor** – Consumes transactions from RabbitMQ published by the Subscriber, parses them and forwards them to the GMP API.
- **Includer** – Consumes tasks from RabbitMQ published by the Distributor and performs the necessary actions on Solana.

A big part of the code which is chain-agnostic and the same across different chain integrations to Axelar can be found in the [relayer-core](https://github.com/commonprefix/axelar-relayer-core) repo. That includes most of the Queue and Database specific code, as well as mechanisms for retries, tracing and other features.

## Solana Terminology:

- **Transactions** - Solana transactions include one or more instructions and they are atomic.
- **Instructions** – Actions to be executed on chain e.g. call a function on a Solana Program.
- **Programs** - Accounts containing executable code which act as Solana's version of Smart Contracts
- **Program Derived Addresses** - PDAs are special addresses you can derive from a Program ID
- **Cross Program Invocation** - CPIs refer to when a Program invokes the instructions of another Program
- **Lamports** - Smallest unit of SOL (1 SOL = 1 billion lamports)

You can read more about these topics in the [Solana Docs](https://solana.com/docs).

## Subscriber

The Subscriber uses the [Solana Rust SDK](https://solana.com/docs/clients/rust) to listen to the chain and employs a dual strategy to fetch transactions that correspond to the following Programs:

- Axelar Gateway Program
- Gas Service Program  
- Interchain Token Transfer (ITS) Program

These program addresses are configured via the `solana_gateway`, `solana_gas_service`, and `solana_its` fields in the configuration file.

### Listener

The first and primary way a subscriber receives relevant transactions is by subscribing to real-time events emitted by the Programs it monitors. On launch, the listener opens 3 different socket connections in parallel, using [Tokio](https://docs.rs/tokio/latest/tokio/) tasks, one for each of the Programs. Upon receiving a transaction, it spawns a new task to parse it and publish it to the Queue, and it also saves it in a PostgreSQL instance.

While real-time streaming is great for low latency, there are pitfalls that need to be avoided. First off, while the subscriber is down (for maintenance, due to restarts etc), all events emitted by the Programs will be missed. Also, socket connection failures are sometimes silent and hard to detect. Thus, we employ a second back-up strategy:

### Poller

There is a separate Task running in the background for each Program, which periodically polls the RPC for any transactions with the addresses of the Programs, using a configurable interval. In case of a failure, it has an exponential back-off retry mechanism. In order to avoid duplicate transaction parsing (in case the listener has already processed this transaction), the poller only processes the transactions it received if they have not been persisted before in the database. There is also a cursor saved which indicates the last transaction that was checked by the poller, so as to only make RPC requests for new transactions (from last_checked up-to-date). The poller and the listener act independently and are agnostic of each other.

You can find the different implementations under `src/subscriber_listener.rs` and `src/subscriber_poller.rs` accordingly.

These two subscriber mechanisms together ensure that all relevant events are processed real-time, and in cases of restarts or connection downtime, we never miss a transaction (or have duplicates). Functions and tasks are non-blocking, to ensure parallel processing and high throughput.

## Ingestor

The Ingestor breaks down transactions from the queue into instructions and runs them through a parser defined in the `transaction_parser`
directory. Each parser must:

1. Determine whether it should handle the transaction.
2. Create an event to send to the GMP API.

The parser is constructed to parse [Anchor's](https://www.anchor-lang.com/docs) `emit_cpi!` events. While normal `sol_log_data` and `emit!` events live in the `logs` field of a transaction, that is not the case for the `emit_cpi!` events. Instead, the program calls itself in an instruction known as Self-CPI ([Cross Program Invocation](https://solana.com/docs/core/cpi)), and encodes the event emitted in the `data` field of that instruction. This is done to prevent various attacks and to provide a more structured way to parse events in place of the normal `logs` which are just Strings. Thus, each parser tries to decode the `data` field and see if it matches the Event struct it expects, while also making security checks (e.g. confirming that the Program that emitted the event matches the expected Program).

### Supported Event Types

The following parsers are implemented:

**Gateway Events:**
- Call Contract (`parser_call_contract.rs`)
- Message Approved (`parser_message_approved.rs`)
- Message Executed (`parser_message_executed.rs`)
- Signers Rotated (`parser_signers_rotated.rs`)

**Gas Service Events:**
- Native Gas Paid (`parser_native_gas_paid.rs`)
- Native Gas Added (`parser_native_gas_added.rs`)
- Native Gas Refunded (`parser_native_gas_refunded.rs`)

**ITS (Interchain Token Service) Events:**
- Interchain Transfer (`parser_its_interchain_transfer.rs`)
- Interchain Token Deployment Started (`parser_its_interchain_token_deployment_started.rs`)
- Link Token Started (`parser_its_link_token_started.rs`)
- Token Metadata Registered (`parser_its_token_metadata_registered.rs`)

### Example: Call Contract Parser

`parser_call_contract.rs` implements `ParserCallContract` with two methods:

- **`is_match`** – Confirms the transaction happened on *our* Axelar Gateway contract and that the correct event was
  emitted, by checking the program_id and the discriminators.
- **`event`** – Extracts the event from the self-CPI and maps it to a GMP API event.

### Assumptions

- A **Gas Paid** event must accompany a **Contract Call** event in the same transaction (although a **Contract Call** event can be emitted on its own, and have its gas be paid later on by a **Gas Added** event).
- The **Call Contract** events are matched with the **Gas Paid** events in the order that they are encountered in the transaction.
- Every **ITS Event** must be paired with a **Call Contract** event in the same transaction.
- Same as before, they are matched based on their order.

To keep in line with EVM relayer, Contract Call events and Gas Paid events are linked by key (determined by payload
hash, destination chain and destination address).
ITS events are only linked to Call Contract events they accompany by the order of their appearance.

To accommodate for connecting events, `event` method on `Parser` optionally accepts `message_id`. If this id is present,
Event's message_id should be set to it. Otherwise, the parser should determine its own `message_id`.

Likewise, a parser that will be connected by key to another one needs to implement `key` method.

Once all events are extracted, they are sent to the GMP API.

## Distributor

The Distributor fetches unseen tasks from the GMP API and enqueues them in RabbitMQ. It lives in the [relayer-core](https://github.com/commonprefix/axelar-relayer-core) repo, as it is the same for every chain integration.

## Includer

The Includer consumes tasks from RabbitMQ and sends corresponding messages to the Solana chain. It has not been implemented yet.

# Setup

### Prerequisites

Ensure the following services are installed and running on your system:
- **Redis Server**  
- **RabbitMQ**
- **PostgreSQL**

### Installation

1. **Clone the Repository**

    ```bash
    git clone https://github.com/axelarnetwork/axelar-relayer-solana.git
    cd axelar-relayer-solana/
    ```

2. **Build the Project**

    Compile the project using Cargo:

    ```bash
    cargo build --release
    ```

3. **Configure Environment and Config Variables**

    Create a `.env` file by copying the provided template and update the necessary configurations:

    ```bash
    cp .env_template .env
    ```

    Open the `.env` file in your preferred text editor and set the environment variables.

    Create a `config.{NETWORK}.yaml` file by copying the provided template, where `NETWORK` can be `localnet`, `devnet`, `testnet` or `mainnet` and update the necessary configurations:

    ```bash
    cp config.template.yaml config.{NETWORK}.yaml
    ```

    Open the `config` file in your preferred text editor and set the environment variables.

4. **Run the migrations for the Database**

    Create a Database called `relayer` in PostgreSQL and then run 

     ```bash
    sqlx migrate run --database-url postgres://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>
    ```
    An common set-up example is: 

     ```bash
    sqlx migrate run --database-url postgres://postgres:postgres@localhost:5432/relayer
    ```





### Running the Components

Each component can be run individually. It's recommended to use separate terminal sessions or a process manager to handle multiple components concurrently.
Chains are run using separate binaries, so adjust the following commands accordingly: 

- **Subscriber**

    ```bash
    cargo run --bin subscriber
    ```

- **Distributor**

    ```bash
    cargo run --bin distributor
    ```

- **Ingestor**

    ```bash
    cargo run --bin ingestor
    ```

- **Includer**

    ```bash
    cargo run --bin includer
    ```