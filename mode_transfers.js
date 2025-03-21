import { keccak256, toHex } from "viem";
import {
  HypersyncClient,
  LogField,
  JoinMode,
  BlockField,
  TransactionField,
} from "@envio-dev/hypersync-client";
import fs from "fs";
import path from "path";

// Define the token address you want to track
const TOKEN_ADDRESS = "0xDfc7C877a950e49D2610114102175A06C2e3167a"; // MODE address

// Define ERC20 Transfer event signature
const event_signatures = [
  "Transfer(address,address,uint256)", // Standard ERC20 Transfer event
];

// Create topic0 hash from event signature
const topic0_list = event_signatures.map((sig) => keccak256(toHex(sig)));

// Initialize Hypersync client
const client = HypersyncClient.new({
  url: "https://mode.hypersync.xyz",
});

// Define query for token transfer events
let query = {
  fromBlock: 11203124,
  logs: [
    {
      address: [TOKEN_ADDRESS],
      topics: [topic0_list],
    },
  ],
  fieldSelection: {
    block: [BlockField.Number, BlockField.Timestamp], // We need these for the CSV
    log: [
      LogField.Data,
      LogField.Address, 
      LogField.Topic0,
      LogField.Topic1,
      LogField.Topic2,
      LogField.BlockNumber,
      LogField.TransactionHash,
    ],
    transaction: [
      TransactionField.From,
      TransactionField.To,
      TransactionField.Hash,
    ],
  },
  joinMode: JoinMode.JoinTransactions,
};

// CSV file setup
const CSV_FILENAME = "mode_transfers.csv";
const CSV_HEADERS = "block_number,timestamp,tx_hash,from_address,to_address,amount\n";

const main = async () => {
  console.log("Starting MODE Transfer event tracking...");

  // Initialize CSV file
  fs.writeFileSync(CSV_FILENAME, CSV_HEADERS);
  
  let totalEvents = 0;
  const startTime = performance.now();

  // Start streaming events
  const stream = await client.stream(query, {});

  // Debug flag - log first log's full structure
  let firstLogPrinted = false;

  while (true) {
    const res = await stream.recv();

    // Exit if we've reached the end of the chain
    if (res === null) {
      console.log("Reached the tip of the blockchain");
      break;
    }

    // Debug: Print first log structure once
    // Debug: Print first log structure once
    if (!firstLogPrinted && res.data && res.data.logs && res.data.logs.length > 0) {
      console.log("First log structure:", JSON.stringify(res.data.logs[0], null, 2));
      console.log("Available keys:", Object.keys(res.data.logs[0]));
      
      // Check if topics are in a different format
      if (res.data.logs[0].topics) {
        console.log("Topics array:", res.data.logs[0].topics);
      }
      
      firstLogPrinted = true;
    }

    // Process events and append to CSV
    if (res.data && res.data.logs) {
      totalEvents += res.data.logs.length;
      
      // Process each transfer event and write to CSV
      for (const log of res.data.logs) {
        try {
          // Extract data from the log
          const blockNumber = log.blockNumber || "";
          const blockData = res.data.blocks?.find(b => b.number === blockNumber);
          const timestamp = blockData?.timestamp || "";
          const txHash = log.transactionHash || "";
          
          // Extract addresses (check if topics exist first)
          let fromAddress = "unknown";
          let toAddress = "unknown";
          let amount = "0";
          
          // Option 1: Direct topic1/topic2 properties
          if (log.topic1) {
            fromAddress = `0x${log.topic1.slice(-40)}`;
          }
          if (log.topic2) {
            toAddress = `0x${log.topic2.slice(-40)}`;
          }

          // Option 2: Check if topics is an array
          if (fromAddress === "unknown" && log.topics && Array.isArray(log.topics) && log.topics.length > 1) {
            fromAddress = `0x${log.topics[1].slice(-40)}`;
          }
          if (toAddress === "unknown" && log.topics && Array.isArray(log.topics) && log.topics.length > 2) {
            toAddress = `0x${log.topics[2].slice(-40)}`;
          }

          // More detailed debug for this specific issue
          if (fromAddress === "unknown" || toAddress === "unknown") {
            console.log("Address extraction failed. Log structure:", log);
          }
          
          // Extract amount - handle 0x prefix properly
          if (log.data) {
            try {
              // Remove '0x' prefix if it exists before adding our own
              const cleanData = log.data.startsWith('0x') ? log.data.slice(2) : log.data;
              amount = BigInt(`0x${cleanData}`).toString();
            } catch (e) {
              console.warn("Error parsing amount:", e, "for data:", log.data);
              // Fallback amount
              amount = "0";
            }
          }
          
          // Create CSV row
          const csvRow = `${blockNumber},${timestamp},${txHash},${fromAddress},${toAddress},${amount}\n`;
          
          // Append to CSV file
          fs.appendFileSync(CSV_FILENAME, csvRow);
        } catch (error) {
          console.warn("Error processing log:", error);
        }
      }
    }

    // Update query for next batch
    if (res.nextBlock) {
      query.fromBlock = res.nextBlock;
    }

    // Calculate and print simple progress metrics
    const seconds = (performance.now() - startTime) / 1000;

    console.log(
      `Block ${res.nextBlock} | ${totalEvents} transfers | ${seconds.toFixed(
        1
      )}s | ${(totalEvents / seconds).toFixed(1)} transfers/s`
    );
  }

  // Print final results
  const totalTime = (performance.now() - startTime) / 1000;
  console.log(
    `\nScan complete: ${totalEvents} transfers in ${totalTime.toFixed(1)} seconds`
  );
  console.log(`CSV file saved as: ${path.resolve(CSV_FILENAME)}`);
};

main().catch((error) => {
  console.error("Error:", error);
  process.exit(1);
});