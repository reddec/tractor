# Tractor

Not very cute, but powerful system for dirty task ;-)

Supports multi-broker and multi-db fail-over

(README is in progress)

## Overview

(in progress)

This is a system based on message AMQP broker (tested on RabbitMQ) for distributed tasks.

Made as combination of ideas from IronFunctinos, CGI and Node-red


## Configurable parameters

(in progress)

### Limit - execution time

Maximum execution time. After this time application will be killed. Available only for non-stream executables.

* **place**: `limits.execution_time`
* **type**: duration
* **default**: no limit

### Limit - graceful delay

Maximum delay after SIGINT signal and before SIGKILL (terminate). Right now available only for non-stream executables

* **place**: `limits.graceful_time`
* **type**: duration
* **default**: 2s