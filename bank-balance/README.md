1 - Create a Kafka Producer that outputs 100 messages per seconds to a topic. Each message
is random in money (a positive value), and outputs evenly transactions for 6 customers. The data should look like this:
{ "name": "bkake", "amount": 123, "time": "2024-04-08T00:00:00Z"}

2 - Create Kafka Streams application that takes these transactions and will compute the total money
in their balance(the balance starts at 0$), and the latest time an update was receive.
As you guessed, the total money is not idempotent(sum), but latest time is (max).

3 - Run the producer and Streams applications

4 - There you go, you have an exactly once pipeline