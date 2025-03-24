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

// Define the contract address to track
// // volatile iusd <> usdc
// const CONTRACT_ADDRESS = "0x0f53E9d4147c2073cc64a70FFc0fec9606E2EEb7";
// // cl iusd <> mode
const CONTRACT_ADDRESS = "0xEC1D7b7058dF61ef9401DB56DbF195388b77EABa";

// Define Mint and Burn event signatures separately
// // volatile pool signatures
// const MINT_EVENT_SIGNATURE = "Mint(address,address,uint256,uint256)";
// const BURN_EVENT_SIGNATURE = "Burn(address,address,uint256,uint256)";
// // cl pool signatures
const MINT_EVENT_SIGNATURE = "Mint(address,address,int24,int24,uint128,uint256,uint256)";
const BURN_EVENT_SIGNATURE = "Burn(address,int24,int24,uint128,uint256,uint256)";

// Create topic0 hash from event signatures separately
const MINT_TOPIC0 = keccak256(toHex(MINT_EVENT_SIGNATURE));
const BURN_TOPIC0 = keccak256(toHex(BURN_EVENT_SIGNATURE));

// Initialize Hypersync client
const client = HypersyncClient.new({
  url: "https://mode.hypersync.xyz",
});

// Define base query structure that can be reused for both event types
const createQuery = (eventTopic0, fromBlock = 11203124) => ({
  fromBlock: fromBlock,
  logs: [
    {
      address: [CONTRACT_ADDRESS],
      topics: [[eventTopic0]],
    },
  ],
  fieldSelection: {
    block: [BlockField.Number, BlockField.Timestamp],
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
});

// CSV file setup
const CSV_FILENAME = "velo_cl_mint_burn_events.csv";
const CSV_HEADERS = "block_number,timestamp,tx_hash,sender_address,to_address,amount0,amount1,event_type\n";

// Function to process and save events for a specific event type
const processEvents = async (eventType, topic0) => {
  console.log(`Starting ${eventType} event tracking...`);
  
  // Initialize query for this event type
  let query = createQuery(topic0);
  
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
      console.log(`Reached the tip of the blockchain for ${eventType} events`);
      break;
    }

    // Debug: Print first log structure once
    if (!firstLogPrinted && res.data && res.data.logs && res.data.logs.length > 0) {
      console.log(`First ${eventType} log structure:`, JSON.stringify(res.data.logs[0], null, 2));
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
      
      // Process each event and write to CSV
      for (const log of res.data.logs) {
        try {
          // Extract data from the log
          const blockNumber = log.blockNumber || "";
          const blockData = res.data.blocks?.find(b => b.number === blockNumber);
          const timestamp = blockData?.timestamp || "";
          const txHash = log.transactionHash || "";
          
          // Extract addresses (check if topics exist first)
          let senderAddress = "unknown";
          let toAddress = "unknown";
          
          // Option 1: Direct topic1/topic2 properties
          if (log.topic1) {
            senderAddress = `0x${log.topic1.slice(-40)}`;
          }
          if (log.topic2) {
            toAddress = `0x${log.topic2.slice(-40)}`;
          }

          // Option 2: Check if topics is an array
          if (senderAddress === "unknown" && log.topics && Array.isArray(log.topics) && log.topics.length > 1) {
            senderAddress = `0x${log.topics[1].slice(-40)}`;
          }
          if (toAddress === "unknown" && log.topics && Array.isArray(log.topics) && log.topics.length > 2) {
            toAddress = `0x${log.topics[2].slice(-40)}`;
          }

          // More detailed debug for this specific issue
          if (senderAddress === "unknown" || toAddress === "unknown") {
            console.log("Address extraction failed. Log structure:", log);
          }
          
          // Extract amounts - the data field contains two uint256 values (amount0 and amount1)
          let amount0 = "0";
          let amount1 = "0";
          
          if (log.data) {
            try {
              // Remove '0x' prefix if it exists
              const cleanData = log.data.startsWith('0x') ? log.data.slice(2) : log.data;
              
              // Each uint256 is 64 characters long in hex
              if (cleanData.length >= 128) {
                amount0 = BigInt(`0x${cleanData.slice(0, 64)}`).toString();
                amount1 = BigInt(`0x${cleanData.slice(64, 128)}`).toString();
              }
            } catch (e) {
              console.warn("Error parsing amounts:", e, "for data:", log.data);
              // Fallback amounts
              amount0 = "0";
              amount1 = "0";
            }
          }
          
          // Create CSV row with the fixed event type
          const csvRow = `${blockNumber},${timestamp},${txHash},${senderAddress},${toAddress},${amount0},${amount1},${eventType}\n`;
          
          // Append to CSV file
          fs.appendFileSync(CSV_FILENAME, csvRow);
        } catch (error) {
          console.warn(`Error processing ${eventType} log:`, error);
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
      `${eventType} | Block ${res.nextBlock} | ${totalEvents} events | ${seconds.toFixed(
        1
      )}s | ${(totalEvents / seconds).toFixed(1)} events/s`
    );
  }

  // Print final results
  const totalTime = (performance.now() - startTime) / 1000;
  console.log(
    `\n${eventType} scan complete: ${totalEvents} events in ${totalTime.toFixed(1)} seconds`
  );
  
  return totalEvents;
};

const main = async () => {
  // Initialize CSV file
  fs.writeFileSync(CSV_FILENAME, CSV_HEADERS);
  
  const startTime = performance.now();
  
  // First process Mint events
  const mintEvents = await processEvents("mint", MINT_TOPIC0);
  
  // Then process Burn events
  const burnEvents = await processEvents("burn", BURN_TOPIC0);
  
  // Print overall results
  const totalTime = (performance.now() - startTime) / 1000;
  console.log(
    `\nOverall scan complete: ${mintEvents + burnEvents} total events in ${totalTime.toFixed(1)} seconds`
  );
  console.log(`CSV file saved as: ${path.resolve(CSV_FILENAME)}`);
};

main().catch((error) => {
  console.error("Error:", error);
  process.exit(1);
});