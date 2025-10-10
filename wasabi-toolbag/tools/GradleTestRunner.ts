// IMPORTANT: Only use node: prefixed imports for Node.js built-ins
import { exec } from "node:child_process";

// Type definition for the context parameter - this is injected by Wasabi
// IMPORTANT: Always include this ToolContext interface in the tool file
interface ToolContext {
  // File system operations
  readonly fs: typeof import("node:fs");
  readonly path: typeof import("node:path");
  readonly os: typeof import("node:os");
  readonly process: typeof import("node:process");

  // HTTP client for internal Amazon services (authenticated with Midway)
  // Use this for making authenticated requests to internal services
  readonly httpClient: {
    request<TInput = unknown, TOutput = unknown>(
      url: URL,
      method: "GET" | "POST" | "PUT" | "DELETE" | "PATCH" | "HEAD",
      options?: {
        timeout?: number;
        retryStrategy?: { maxAttempts: number; maxElapsedTime: number };
        body?: TInput;
        headers?: Record<string, string>;
        compression?: "gzip" | "br";
        doNotParse?: TOutput extends Buffer ? boolean : never;
      }
    ): Promise<{
      statusCode: number;
      headers: Record<string, string | string[] | undefined>;
      body: TOutput
    }>;
  };
  readonly rootDir: string;
  readonly validFileGlobs: string[];
  readonly excludedFileGlobs: string[];

  readonly bedrock: {
    prompt(promptParams: {
      inputs: BedrockMessage[];
      system?: { text: string }[];
      inferenceConfig?: {
        maxTokens?: number;
        temperature?: number;
        topP?: number;
      };
    }): Promise<{
      stopReason?: string;
      tokensUsed?: number;
      // This will include inputs and new messages from inference
      messages: BedrockMessage[];
    }>;
  }
}

// IMPORTANT: Always include this type in the tool file
type BedrockMessage = {
  role: "user" | "assistant" | string;
  content: Array<{
    text?: string;
    document?: {
      name: string;
      content: string;
    };
    toolUse?: {
      name: string;
      input: string;
    };
    toolResult?: {
      name: string;
      status: "success" | "error";
      content: Array<{
        text?: string;
        document?: {
          name: string;
          content: string;
        };
      }>;
    };
  }>;
};

// CRITICAL: Define a strict interface for your tool's parameters
interface GradleTestRunnerParams {
  module?: string;
  testCategory?: "unit" | "integration" | "e2e" | "all";
  pattern?: string;
  includeIntegrationTest?: boolean;
  verbose?: boolean;
}

/**
 * IMPORTANT IMPLEMENTATION REQUIREMENTS:
 * 1. Tool MUST be the default export
 * 2. Tool MUST be a class (not a function or object)
 * 3. Class name MUST match the tool name property
 * 4. Tool name MUST be unique across all tools (including built-in tools)
 * 5. Tool name MUST only contain letters (a-z, A-Z), numbers (0-9), underscores (_), and hyphens (-)
 * 6. Tool MUST have an execute method
 * 7. Tool MUST have an inputSchema with a json property containing the JSON Schema
 */
class GradleTestRunner {
  // REQUIRED: Constructor must accept ToolContext
  constructor(private readonly context: ToolContext) {}

  // REQUIRED: Name property should match the class name
  public readonly name = "GradleTestRunner";

  // REQUIRED: Schema defining the expected input parameters
  public readonly inputSchema = {
    json: {
      type: "object",
      properties: {
        module: {
          type: "string",
          description: "Specific module to test (e.g., 'data-prepper-core', 'data-prepper-plugins/s3-sink'). If not specified, runs tests for all modules"
        },
        testCategory: {
          type: "string",
          enum: ["unit", "integration", "e2e", "all"],
          description: "Category of tests to run (unit, integration, e2e, or all)",
          default: "unit"
        },
        pattern: {
          type: "string",
          description: "Test class name pattern to match (e.g., '*ProcessorTest', 'MySpecificTest')"
        },
        includeIntegrationTest: {
          type: "boolean",
          description: "Whether to include integration tests in addition to unit tests",
          default: false
        },
        verbose: {
          type: "boolean",
          description: "Enable verbose output including detailed test results",
          default: false
        }
      },
      additionalProperties: false
    }
  } as const;

  // REQUIRED: Description of what the tool does
  public readonly description =
    "Intelligently runs Gradle tests for the data-prepper project with filtering by module, test category, and patterns. Parses and formats JUnit XML results for analysis.";

  // REQUIRED: execute method that implements the tool's functionality
  public async execute(params: GradleTestRunnerParams) {
    const {
      module,
      testCategory = "unit",
      pattern,
      includeIntegrationTest = false,
      verbose = false
    } = params;

    try {
      // Build Gradle command
      let gradleCommand = "./gradlew";

      // Determine test tasks based on category
      const testTasks: string[] = [];

      if (testCategory === "unit" || testCategory === "all") {
        testTasks.push("test");
      }

      if (testCategory === "integration" || testCategory === "all" || includeIntegrationTest) {
        testTasks.push("integrationTest");
      }

      if (testCategory === "e2e" || testCategory === "all") {
        testTasks.push(":e2e-test:integrationTest");
      }

      // Add module prefix if specified
      if (module) {
        const modulePrefix = module.startsWith(":") ? module : `:${module}`;
        gradleCommand += testTasks.map(task => `${modulePrefix}:${task}`).join(" ");
      } else {
        gradleCommand += " " + testTasks.join(" ");
      }

      // Add test pattern if specified
      if (pattern) {
        gradleCommand += ` --tests "${pattern}"`;
      }

      // Add verbose flags if requested
      if (verbose) {
        gradleCommand += " --info";
      }

      // Add parallel execution for better performance
      gradleCommand += " --parallel --max-workers=4";

      console.log(`Executing: ${gradleCommand}`);

      return new Promise<any>((resolve) => {
        exec(gradleCommand, {
          cwd: this.context.rootDir,
          maxBuffer: 1024 * 1024 * 10 // 10MB buffer for large outputs
        }, (error, stdout, stderr) => {
          if (error) {
            // Parse test results even on failure
            const testResults = this.parseTestOutput(stdout, stderr);
            resolve({
              status: "error",
              message: "Test execution failed with errors",
              command: gradleCommand,
              testResults,
              error: error.message,
              stdout: verbose ? stdout : this.extractTestSummary(stdout),
              stderr
            });
          } else {
            const testResults = this.parseTestOutput(stdout, stderr);
            resolve({
              status: "success",
              message: "Tests completed successfully",
              command: gradleCommand,
              testResults,
              stdout: verbose ? stdout : this.extractTestSummary(stdout)
            });
          }
        });
      });
    } catch (err) {
      return {
        status: "error",
        message: "Failed to execute Gradle test command",
        error: err instanceof Error ? err.message : String(err)
      };
    }
  }

  private parseTestOutput(stdout: string, stderr: string): any {
    const results: any = {
      summary: {
        total: 0,
        passed: 0,
        failed: 0,
        skipped: 0
      },
      failures: [],
      coverage: null
    };

    // Parse test summary from Gradle output
    const summaryMatch = stdout.match(/(\d+) tests completed(?:, (\d+) failed)?(?:, (\d+) skipped)?/);
    if (summaryMatch) {
      results.summary.total = parseInt(summaryMatch[1]);
      results.summary.failed = summaryMatch[2] ? parseInt(summaryMatch[2]) : 0;
      results.summary.skipped = summaryMatch[3] ? parseInt(summaryMatch[3]) : 0;
      results.summary.passed = results.summary.total - results.summary.failed - results.summary.skipped;
    }

    // Extract test failures
    const failureMatches = stdout.matchAll(/FAILED.*?> (.+?)(?:\n|$)/g);
    for (const match of failureMatches) {
      results.failures.push(match[1]);
    }

    // Extract JaCoCo coverage if present
    const coverageMatch = stdout.match(/Total.*?(\d+(?:\.\d+)?)%/);
    if (coverageMatch) {
      results.coverage = coverageMatch[1] + "%";
    }

    return results;
  }

  private extractTestSummary(output: string): string {
    const lines = output.split('\n');
    const summaryLines: string[] = [];

    let inTestResults = false;
    for (const line of lines) {
      if (line.includes('BUILD SUCCESSFUL') || line.includes('BUILD FAILED')) {
        summaryLines.push(line);
        break;
      }
      if (line.includes('tests completed') || line.includes('FAILED') || line.includes('Total coverage')) {
        summaryLines.push(line);
        inTestResults = true;
      }
      if (inTestResults && (line.startsWith('> Task :') || line.includes('Test result:'))) {
        summaryLines.push(line);
      }
    }

    return summaryLines.join('\n');
  }
}

// REQUIRED: Default export must be the tool class
export default GradleTestRunner;
