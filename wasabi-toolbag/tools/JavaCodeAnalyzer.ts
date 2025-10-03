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
interface JavaCodeAnalyzerParams {
  analysisType?: "checkstyle" | "coverage" | "dependencies" | "logging" | "all";
  module?: string;
  outputFormat?: "summary" | "detailed";
  includeRecommendations?: boolean;
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
class JavaCodeAnalyzer {
  // REQUIRED: Constructor must accept ToolContext
  constructor(private readonly context: ToolContext) {}

  // REQUIRED: Name property should match the class name
  public readonly name = "JavaCodeAnalyzer";

  // REQUIRED: Schema defining the expected input parameters
  public readonly inputSchema = {
    json: {
      type: "object",
      properties: {
        analysisType: {
          type: "string",
          enum: ["checkstyle", "coverage", "dependencies", "logging", "all"],
          description: "Type of analysis to perform",
          default: "all"
        },
        module: {
          type: "string",
          description: "Specific module to analyze (e.g., 'data-prepper-core'). If not specified, analyzes all modules"
        },
        outputFormat: {
          type: "string",
          enum: ["summary", "detailed"],
          description: "Format of the analysis output",
          default: "summary"
        },
        includeRecommendations: {
          type: "boolean",
          description: "Include actionable recommendations in the analysis",
          default: true
        }
      },
      additionalProperties: false
    }
  } as const;

  // REQUIRED: Description of what the tool does
  public readonly description =
    "Comprehensive code quality analysis for Java modules including CheckStyle violations, code coverage analysis, dependency analysis, and logging pattern validation.";

  // REQUIRED: execute method that implements the tool's functionality
  public async execute(params: JavaCodeAnalyzerParams) {
    const {
      analysisType = "all",
      module,
      outputFormat = "summary",
      includeRecommendations = true
    } = params;

    try {
      const analysis: any = {
        timestamp: new Date().toISOString(),
        module: module || "all",
        analysisType,
        results: {}
      };

      if (analysisType === "checkstyle" || analysisType === "all") {
        analysis.results.checkstyle = await this.analyzeCheckstyle(module);
      }

      if (analysisType === "coverage" || analysisType === "all") {
        analysis.results.coverage = await this.analyzeCoverage(module);
      }

      if (analysisType === "dependencies" || analysisType === "all") {
        analysis.results.dependencies = await this.analyzeDependencies(module);
      }

      if (analysisType === "logging" || analysisType === "all") {
        analysis.results.logging = await this.analyzeLogging(module);
      }

      if (includeRecommendations) {
        analysis.recommendations = this.generateRecommendations(analysis.results);
      }

      return {
        status: "success",
        message: "Code analysis completed successfully",
        analysis: outputFormat === "summary" ? this.summarizeResults(analysis) : analysis
      };

    } catch (err) {
      return {
        status: "error",
        message: "Failed to perform code analysis",
        error: err instanceof Error ? err.message : String(err)
      };
    }
  }

  private async analyzeCheckstyle(module?: string): Promise<any> {
    return new Promise((resolve) => {
      const command = module
        ? `./gradlew :${module}:checkstyleMain checkstyleTest`
        : "./gradlew checkstyleMain checkstyleTest";

      exec(command, { cwd: this.context.rootDir }, (error, stdout, stderr) => {
        const results = {
          violations: [],
          summary: { total: 0, errors: 0, warnings: 0 },
          passed: !error
        };

        if (stdout) {
          // Parse checkstyle violations
          const violationMatches = stdout.matchAll(/\[ant:checkstyle\] (.+?):(\d+):(?:(\d+):)? (.+?) \[(.+?)\]/g);
          for (const match of violationMatches) {
            results.violations.push({
              file: match[1],
              line: parseInt(match[2]),
              column: match[3] ? parseInt(match[3]) : null,
              message: match[4],
              rule: match[5]
            });
          }
          results.summary.total = results.violations.length;
        }

        resolve(results);
      });
    });
  }

  private async analyzeCoverage(module?: string): Promise<any> {
    return new Promise((resolve) => {
      const command = module
        ? `./gradlew :${module}:jacocoTestReport`
        : "./gradlew jacocoTestReport";

      exec(command, { cwd: this.context.rootDir }, (error, stdout, stderr) => {
        const results = {
          modules: [],
          overallCoverage: null,
          belowThreshold: []
        };

        // Try to find and parse JaCoCo XML reports
        this.findCoverageReports(module).then(reports => {
          results.modules = reports;
          results.overallCoverage = this.calculateOverallCoverage(reports);
          results.belowThreshold = reports.filter(r => r.lineCoverage < 65);
          resolve(results);
        }).catch(() => {
          resolve(results);
        });
      });
    });
  }

  private async findCoverageReports(module?: string): Promise<any[]> {
    const reports: any[] = [];
    const searchPaths = module ? [`${module}/build/reports/jacoco`] : ['**/build/reports/jacoco'];

    try {
      for (const searchPath of searchPaths) {
        const fullPath = this.context.path.join(this.context.rootDir, searchPath);
        if (this.context.fs.existsSync(fullPath)) {
          // Parse XML coverage reports (simplified)
          const xmlPath = this.context.path.join(fullPath, 'test/jacocoTestReport.xml');
          if (this.context.fs.existsSync(xmlPath)) {
            const content = this.context.fs.readFileSync(xmlPath, 'utf8');
            const coverageMatch = content.match(/type="LINE".*?covered="(\d+)".*?missed="(\d+)"/);
            if (coverageMatch) {
              const covered = parseInt(coverageMatch[1]);
              const missed = parseInt(coverageMatch[2]);
              const total = covered + missed;
              reports.push({
                module: module || "unknown",
                lineCoverage: total > 0 ? Math.round((covered / total) * 100) : 0,
                covered,
                missed,
                total
              });
            }
          }
        }
      }
    } catch (error) {
      // Ignore errors, return empty reports
    }

    return reports;
  }

  private calculateOverallCoverage(reports: any[]): number | null {
    if (reports.length === 0) return null;

    const totals = reports.reduce((acc, report) => ({
      covered: acc.covered + report.covered,
      total: acc.total + report.total
    }), { covered: 0, total: 0 });

    return totals.total > 0 ? Math.round((totals.covered / totals.total) * 100) : 0;
  }

  private async analyzeDependencies(module?: string): Promise<any> {
    return new Promise((resolve) => {
      const command = module
        ? `./gradlew :${module}:dependencies`
        : "./gradlew dependencies";

      exec(command, { cwd: this.context.rootDir }, (error, stdout, stderr) => {
        const results = {
          outdated: [],
          security: [],
          conflicts: [],
          analysis: "Basic dependency analysis completed"
        };

        if (stdout) {
          // Look for version conflicts
          const conflictMatches = stdout.matchAll(/\+--- (.+?) -> (.+)/g);
          for (const match of conflictMatches) {
            results.conflicts.push({
              dependency: match[1],
              resolvedTo: match[2]
            });
          }
        }

        resolve(results);
      });
    });
  }

  private async analyzeLogging(module?: string): Promise<any> {
    const results = {
      slf4jUsage: 0,
      systemOutUsage: 0,
      logLevels: { error: 0, warn: 0, info: 0, debug: 0, trace: 0 },
      issues: []
    };

    try {
      const searchPattern = module ? `${module}/src/**/*.java` : "**/src/**/*.java";
      const javaFiles = this.findJavaFiles(searchPattern);

      for (const file of javaFiles) {
        const content = this.context.fs.readFileSync(file, 'utf8');

        // Check for SLF4J usage
        if (content.includes('LoggerFactory.getLogger')) {
          results.slf4jUsage++;
        }

        // Check for System.out usage (should be avoided)
        if (content.includes('System.out.')) {
          results.systemOutUsage++;
          results.issues.push({
            file,
            issue: "System.out usage detected",
            recommendation: "Use SLF4J logger instead"
          });
        }

        // Count log levels
        results.logLevels.error += (content.match(/\.error\(/g) || []).length;
        results.logLevels.warn += (content.match(/\.warn\(/g) || []).length;
        results.logLevels.info += (content.match(/\.info\(/g) || []).length;
        results.logLevels.debug += (content.match(/\.debug\(/g) || []).length;
        results.logLevels.trace += (content.match(/\.trace\(/g) || []).length;
      }
    } catch (error) {
      // Ignore errors, return partial results
    }

    return results;
  }

  private findJavaFiles(pattern: string): string[] {
    // Simplified file finding - in practice this would use glob patterns
    const files: string[] = [];
    try {
      const walkDir = (dir: string) => {
        const items = this.context.fs.readdirSync(dir);
        for (const item of items) {
          const fullPath = this.context.path.join(dir, item);
          const stat = this.context.fs.statSync(fullPath);
          if (stat.isDirectory() && !item.startsWith('.') && item !== 'node_modules') {
            walkDir(fullPath);
          } else if (item.endsWith('.java')) {
            files.push(fullPath);
          }
        }
      };
      walkDir(this.context.rootDir);
    } catch (error) {
      // Ignore errors
    }
    return files.slice(0, 100); // Limit to prevent overwhelming output
  }

  private generateRecommendations(results: any): string[] {
    const recommendations: string[] = [];

    if (results.checkstyle?.violations?.length > 0) {
      recommendations.push(`Fix ${results.checkstyle.violations.length} CheckStyle violations to improve code quality`);
    }

    if (results.coverage?.belowThreshold?.length > 0) {
      recommendations.push(`Increase test coverage for ${results.coverage.belowThreshold.length} modules below 65% threshold`);
    }

    if (results.logging?.systemOutUsage > 0) {
      recommendations.push(`Replace ${results.logging.systemOutUsage} System.out usages with SLF4J logging`);
    }

    if (results.dependencies?.conflicts?.length > 0) {
      recommendations.push(`Review ${results.dependencies.conflicts.length} dependency conflicts`);
    }

    return recommendations;
  }

  private summarizeResults(analysis: any): any {
    return {
      timestamp: analysis.timestamp,
      module: analysis.module,
      summary: {
        checkstyleViolations: analysis.results.checkstyle?.violations?.length || 0,
        overallCoverage: analysis.results.coverage?.overallCoverage,
        modulesBelowCoverageThreshold: analysis.results.coverage?.belowThreshold?.length || 0,
        dependencyConflicts: analysis.results.dependencies?.conflicts?.length || 0,
        loggingIssues: analysis.results.logging?.issues?.length || 0
      },
      recommendations: analysis.recommendations || []
    };
  }
}

// REQUIRED: Default export must be the tool class
export default JavaCodeAnalyzer;
