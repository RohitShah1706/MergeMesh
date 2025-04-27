const fs = require("fs");
const path = require("path");
const axios = require("axios");

// Server URL mapping
const serverUrls = {
  SQL: "http://localhost:5000",
  HIVE: "http://localhost:8081",
  MONGO: "http://localhost:5002",
};

// Function to parse a line
function parseLine(line) {
  const regex = /^(\w+)\.(\w+)\((.*?)\)$/;
  const match = line.match(regex);

  if (match) {
    const serverName = match[1];
    const command = match[2];
    const args = match[3] ? match[3].split(",") : [];
    return { serverName, command, args };
  } else {
    console.error(`Failed to parse line: ${line}`);
    return null;
  }
}

// API Call Functions
async function insert(url, args) {
  const body = {
    studentId: args[0],
    courseId: args[1],
    grade: args[2],
  };
  await axios.post(`${url}/`, body);
  console.log(`Inserted into ${url}:`, body);
}

async function update(url, args) {
  const body = {
    studentId: args[0],
    courseId: args[1],
    newGrade: args[2],
  };
  await axios.put(`${url}/`, body);
  console.log(`Updated at ${url}:`, body);
}

async function read(url, args) {
  const params = {
    studentId: args[0],
    courseId: args[1],
  };
  await axios.get(`${url}/`, { params });
  console.log(`Read from ${url} with params:`, params);
}

async function merge(url, args) {
  const body = {
    server: serverUrls[args[0]],
  };
  await axios.post(`${url}/merge`, body);
  console.log(`Merged at ${url}:`, body);
}

// Dispatcher function
async function handleCommand(serverName, command, args) {
  const url = serverUrls[serverName];
  if (!url) {
    console.error(`Unknown server: ${serverName}`);
    return;
  }

  try {
    switch (command) {
      case "INSERT":
        await insert(url, args);
        break;
      case "UPDATE":
        await update(url, args);
        break;
      case "READ":
        await read(url, args);
        break;
      case "MERGE":
        await merge(url, args);
        break;
      default:
        console.error(`Unknown command: ${command}`);
    }
  } catch (error) {
    console.error(
      `Error executing ${command} on ${serverName}:`,
      error.message
    );
  }
}

// Main function
async function main() {
  const fileName = process.argv[2]; // Get the file name from the CLI arguments
  if (!fileName) {
    console.error("Usage: node parser.js <filename>");
    process.exit(1);
  }

  const filePath = path.join(__dirname, fileName);

  const data = fs.readFileSync(filePath, "utf8");
  const lines = data
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0);

  for (const line of lines) {
    const parsed = parseLine(line);
    if (parsed) {
      await handleCommand(parsed.serverName, parsed.command, parsed.args);
    }
  }
}

main();
