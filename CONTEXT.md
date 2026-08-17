# Payment Intermediation

This context accepts payment requests and records their processing by one of two external Payment Processors. Its language distinguishes durable acceptance from confirmed external processing.

## Language

**Payment**:
A request to process a monetary amount, uniquely identified by its Correlation ID and carrying its accepted timestamp.
_Avoid_: Transaction, job, message

**Correlation ID**:
The UUID that identifies one Payment across this backend and the Payment Processors.
_Avoid_: Payment ID, request ID

**Accepted Payment**:
A Payment durably recorded by this backend but not yet confirmed as processed by a Payment Processor.
_Avoid_: Processed payment, completed payment

**Completed Payment**:
A Payment whose processing is confirmed by a Payment Processor and whose confirmed processor is durably recorded by this backend.
_Avoid_: Accepted payment, queued payment

**Payment Processor**:
An external service that records a Payment and charges a fee for doing so.
_Avoid_: Gateway, provider

**Default Processor**:
The Payment Processor with the lower fee. It is preferred whenever it is available.
_Avoid_: Primary processor

**Fallback Processor**:
The Payment Processor with the higher fee, used when the Default Processor is unavailable.
_Avoid_: Secondary processor, backup processor

**Processor Assignment**:
The selected Payment Processor for an Accepted Payment before its first processing attempt. It remains fixed after an ambiguous attempt.
_Avoid_: Routing decision, retry target

**Processor Availability**:
The current assessment of whether a Payment Processor may receive newly assigned Payments.
_Avoid_: Health check, uptime

**Payment Summary**:
The audit view of Completed Payments, grouped by confirmed Payment Processor and filtered by each Payment's accepted timestamp.
_Avoid_: Report, aggregate
