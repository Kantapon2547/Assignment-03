const express = require("express");
const fs = require("fs");
const amqp = require('amqplib');

if (!process.env.PORT) {
    throw new Error("Please specify the port number for the HTTP server with the environment variable PORT.");
}

if (!process.env.RABBIT) {
    throw new Error("Please specify the name of the RabbitMQ host using environment variable RABBIT");
}

const PORT = process.env.PORT;
const RABBIT = process.env.RABBIT;

async function main() {

    console.log(`Connecting to RabbitMQ server at ${RABBIT}.`);
    const messagingConnection = await amqp.connect(RABBIT);
    console.log("Connected to RabbitMQ.");

    const messageChannel = await messagingConnection.createChannel();
	await messageChannel.assertExchange("viewed", "fanout");

	// Updated to broadcast the videoId specifically
	function broadcastViewedMessage(messageChannel, videoId) {
	    console.log(`Publishing 'viewed' message for video ${videoId} on exchange.`);
	    const msg = { videoId: videoId }; // Send the ID so History knows what to save
	    const jsonMsg = JSON.stringify(msg);
	    messageChannel.publish("viewed", "", Buffer.from(jsonMsg));
	}

    const app = express();

    app.get("/video", async (req, res) => {
        // 1. Capture the ID from the URL (?id=1 or ?id=2)
        const videoId = req.query.id;

        if (!videoId) {
            return res.status(400).send("Missing video ID. Use /video?id=1");
        }

        // 2. Map the ID to the correct file
        // IMPORTANT: Ensure you have video1.mp4 and video2.mp4 in your /videos folder
        const videoPath = `./videos/video${videoId}.mp4`;

        try {
            const stats = await fs.promises.stat(videoPath);

            res.writeHead(200, {
                "Content-Length": stats.size,
                "Content-Type": "video/mp4",
            });

            fs.createReadStream(videoPath).pipe(res);

            // 3. Broadcast the ID to other services
            const msg = { videoId: videoId };
            messageChannel.publish("viewed", "", Buffer.from(JSON.stringify(msg)));
            console.log(`Streaming video: ${videoId}`);

        } catch (err) {
            console.error(`Error: File not found at ${videoPath}`);
            res.status(404).send("Video file not found. Check if video1.mp4 exists in the videos folder.");
        }
    });

    // Fix: Listen on 0.0.0.0 to prevent "Connection Reset" in Docker
    app.listen(PORT, "0.0.0.0", () => {
        console.log(`Video-Streaming microservice online on port ${PORT}.`);
    });
}

main()
    .catch(err => {
        console.error("Microservice failed to start.");
        console.error(err && err.stack || err);
    });