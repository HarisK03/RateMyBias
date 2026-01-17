import PocketBase from "pocketbase";
import axios, { type AxiosInstance, type AxiosResponse } from "axios";
import "dotenv/config";
import fs from "fs";

/**
 * CONFIGURATION
 * PB_BATCH_SIZE is now 50 to prevent PocketHost timeouts.
 */
const PB_BATCH_SIZE = 500;
const RMP_FETCH_SIZE = 500;
const PB_URL = "https://rankmyprof.pockethost.io";
const PB_EMAIL = process.env.PB_EMAIL!;
const PB_PASS = process.env.PB_PASS!;
const MAX_RMP_RESULTS = 1000;
const STATE_FILE = "scrape_state.json";

const HEADERS = {
	"User-Agent":
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	Authorization: `Basic ${Buffer.from("test:test").toString("base64")}`,
};

interface TeacherRecord {
	id: string;
	rmpId: string;
	legacyId: number;
	firstName: string;
	lastName: string;
	avgRating: number;
	avgDifficulty: number;
	numRatings: number;
	wouldTakeAgainPercent: number;
	department: string;
	schoolId: string;
	slug: string;
}

interface RMPGraphQLResponse {
	data: {
		search: {
			teachers: {
				pageInfo: { hasNextPage: boolean; endCursor: string | null };
				edges: any[];
				resultCount: number;
			};
		};
	};
}

class RMPTeacherScraper {
	private pb: PocketBase;
	private client: AxiosInstance;
	private processedIds: Set<number> = new Set();
	private uploadQueue: TeacherRecord[] = [];
	private chars = "abcdefghijklmnopqrstuvwxyz0123456789".split("");
	private lastProcessedPrefix = "";

	constructor(pbUrl: string) {
		this.pb = new PocketBase(pbUrl);
		this.client = axios.create({
			baseURL: "https://www.ratemyprofessors.com/graphql",
			headers: HEADERS,
			timeout: 60000, // Increased timeout for RMP
		});
		this.loadState();
	}

	private loadState() {
		if (fs.existsSync(STATE_FILE)) {
			const data = JSON.parse(fs.readFileSync(STATE_FILE, "utf-8"));
			this.lastProcessedPrefix = data.lastPrefix || "";
			console.log(`Resuming from prefix: "${this.lastProcessedPrefix}"`);
		}
	}

	private saveState(prefix: string) {
		fs.writeFileSync(STATE_FILE, JSON.stringify({ lastPrefix: prefix }));
	}

	async init(): Promise<void> {
		try {
			await this.pb
				.collection("users")
				.authWithPassword(PB_EMAIL, PB_PASS);
			console.log("Authenticated with PocketBase.");
		} catch (error: any) {
			console.error("Auth failed:", error.message);
			process.exit(1);
		}
	}

	private transform(node: any): TeacherRecord {
		const graphqlId = Buffer.from(`Teacher-${node.legacyId}`).toString(
			"base64"
		);
		return {
			id: graphqlId,
			rmpId: graphqlId,
			legacyId: node.legacyId,
			firstName: node.firstName || "",
			lastName: node.lastName || "",
			avgRating: node.avgRating || 0,
			avgDifficulty: node.avgDifficulty || 0,
			numRatings: node.numRatings || 0,
			wouldTakeAgainPercent: Math.round(node.wouldTakeAgainPercent) || 0,
			department: node.department || "Unknown",
			schoolId: node.school?.id || "",
			slug: `${node.firstName}-${node.lastName}-${node.legacyId}`
				.toLowerCase()
				.replace(/[^a-z0-9]+/g, "-"),
		};
	}

	async flushQueue(force: boolean = false): Promise<void> {
		while (
			this.uploadQueue.length >= PB_BATCH_SIZE ||
			(force && this.uploadQueue.length > 0)
		) {
			const chunk = this.uploadQueue.slice(0, PB_BATCH_SIZE);
			const batch = this.pb.createBatch();
			chunk.forEach((r) => batch.collection("teachers").upsert(r));

			try {
				await batch.send();
				this.uploadQueue.splice(0, PB_BATCH_SIZE);
				console.log(
					`Batch Success: ${chunk.length} uploaded. Queue: ${this.uploadQueue.length}`
				);
			} catch (error: any) {
				console.error(
					"PocketBase Timeout. Indexing may be slow. Retrying in 15s..."
				);
				await new Promise((r) => setTimeout(r, 15000));
				// We don't splice here, so the next loop iteration retries the same chunk
			}
		}
	}

	async searchRecursive(queryText: string): Promise<void> {
		// RESUME LOGIC: Skip prefixes we already finished
		if (
			this.lastProcessedPrefix &&
			queryText <= this.lastProcessedPrefix &&
			queryText.length <= this.lastProcessedPrefix.length
		) {
			if (!this.lastProcessedPrefix.startsWith(queryText)) return;
		}

		console.log(`Working on: "${queryText}"`);
		let hasNextPage = true;
		let cursor: string | null = null;

		while (hasNextPage) {
			try {
				const response: AxiosResponse<RMPGraphQLResponse> =
					await this.client.post("", {
						query: `query TeacherSearch($count: Int!, $cursor: String, $queryText: String!) {
                        search: newSearch {
                            teachers(query: {text: $queryText}, first: $count, after: $cursor) {
                                resultCount
                                pageInfo { hasNextPage endCursor }
                                edges {
                                    node {
                                        id legacyId firstName lastName avgRating
                                        avgDifficulty numRatings wouldTakeAgainPercent
                                        department school { id }
                                    }
                                }
                            }
                        }
                    }`,
						variables: { count: RMP_FETCH_SIZE, cursor, queryText },
					});

				const results = response.data.data.search.teachers;

				// DRILL DOWN LOGIC
				if (
					cursor === null &&
					results.resultCount >= MAX_RMP_RESULTS &&
					queryText.length < 3
				) {
					console.warn(
						`Prefix "${queryText}" capped. Drilling deeper...`
					);
					for (const char of this.chars) {
						await this.searchRecursive(queryText + char);
					}
					this.saveState(queryText);
					return;
				}

				if (results.edges?.length) {
					results.edges.forEach((e) => {
						if (!this.processedIds.has(e.node.legacyId)) {
							this.uploadQueue.push(this.transform(e.node));
							this.processedIds.add(e.node.legacyId);
						}
					});
					await this.flushQueue();
				}

				hasNextPage = results.pageInfo.hasNextPage;
				cursor = results.pageInfo.endCursor;
				await new Promise((r) => setTimeout(r, 200));
			} catch (error: any) {
				console.error(`Fetch error [${queryText}]: ${error.message}`);
				await new Promise((r) => setTimeout(r, 5000));
			}
		}

		this.saveState(queryText);
	}

	async run(): Promise<void> {
		for (const char of this.chars) {
			await this.searchRecursive(char);
		}
		await this.flushQueue(true);
		console.log("Scrape Complete.");
	}
}

const scraper = new RMPTeacherScraper(PB_URL);
await scraper.init();
await scraper.run();
