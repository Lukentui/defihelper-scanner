import dotenv from "dotenv";
dotenv.config();

export default {
  sentryDsn: "https://1ae51bdc9ea14a02a94c74b544063317@o934824.ingest.sentry.io/6293418",
  database: {
    host: process.env.DATABASE_HOST ?? "localhost",
    port: parseInt(process.env.DATABASE_PORT ?? "5432", 10),
    user: process.env.DATABASE_USER ?? "",
    password: process.env.DATABASE_PASSWORD ?? "",
    database: process.env.DATABASE_NAME ?? "",
    ssl: process.env.DATABASE_SSL ?? "",
  },
  blockchain: {
    ethMainNode: process.env.ETH_NODE ?? "",
    bscMainNode: process.env.BSC_NODE ?? "",
    polygonMainNode: process.env.POLYGON_NODE ?? "",
    moonriverMainNode: process.env.MOONRIVER_NODE ?? "",
    avalancheMainNode: process.env.AVALANCHE_NODE ?? "",
  },
};
