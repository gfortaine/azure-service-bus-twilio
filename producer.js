// producer.js

// Node.js packages used to communicate with Azure Service Bus
const { ServiceBusClient } = require("@azure/service-bus");

// Load the .env file if it exists
require("dotenv").config();

// Define connection string and related Service Bus entity names here
const connectionString =
  process.env.SERVICEBUS_CONNECTION_STRING || "<connection string>";
const queueName = process.env.QUEUE_NAME || "<queue name>";

// Node.js package used to read data from .csv files
const csv = require("csvtojson");

// Node.js package used to work with dates and times
const { Temporal } = require("proposal-temporal");

// send the reminder "x" minutes before the appointment time
const leadTimeInMinutes = 15;

async function main() {
  const sbClient = new ServiceBusClient(connectionString);

  // createSender() can also be used to create a sender for a topic.
  const sender = sbClient.createSender(queueName);

  try {
    // Tries to send all messages in a single batch.
    // Will fail if the messages cannot fit in a batch.
    // import appointments data from .csv file
    const appointmentsData = await csv().fromFile("./appointments.csv");

    for (const row of appointmentsData) {
      const mqData = {
        to: row["Phone"],
        body: `Hello ${row["Name"]}, you have an appointment with us in ${leadTimeInMinutes}
minutes. See you soon.`,
      };

      const instant = toDelayFromDate(row["AppointmentDateTime"]);

      if (
        instant.since(
          Temporal.now.instant()
        ).sign > 0
      ) {
        const scheduledEnqueueTimeUtc = new Date(
          instant.epochMilliseconds
        );

        console.log(instant.toLocaleString());
        console.log("publish...");

        await sender.scheduleMessages(mqData, scheduledEnqueueTimeUtc);
      }
    }

    console.log(`Sent a batch of messages to the queue: ${queueName}`);

    // Close the sender
    await sender.close();
  } finally {
    await sbClient.close();
  }
}

// utility function, returns milliseconds
// calculates the difference between the appointment time and the current time
function toDelayFromDate(dateTime) {
  const timeZone = Temporal.now.timeZone();
  const instant = Temporal.PlainDateTime.from(dateTime)
    .toZonedDateTime(timeZone)
    .toInstant()
    .subtract({ minutes: leadTimeInMinutes });

  return instant;
}

main().catch((err) => {
  console.log("Error occurred: ", err);
  process.exit(1);
});

