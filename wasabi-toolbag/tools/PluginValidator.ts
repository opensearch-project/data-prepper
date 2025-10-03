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
interface PluginValidatorParams {
  pluginPath: string;
  validationType?: "structure" | "annotations" | "integration" | "all";
  strictMode?: boolean;
  generateReport?: boolean;
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
class PluginValidator {
  // REQUIRED: Constructor must accept ToolContext
  constructor(private readonly context: ToolContext) {}

  // REQUIRED: Name property should match the class name
  public readonly name = "PluginValidator";

  // REQUIRED: Schema defining the expected input parameters
  public readonly inputSchema = {
    json: {
      type: "object",
      properties: {
        pluginPath: {
          type: "string",
          description: "Path to the plugin directory (e.g., 'data-prepper-plugins/s3-sink')"
        },
        validationType: {
          type: "string",
          enum: ["structure", "annotations", "integration", "all"],
          description: "Type of validation to perform",
          default: "all"
        },
        strictMode: {
          type: "boolean",
          description: "Enable strict validation with additional checks",
          default: false
        },
        generateReport: {
          type: "boolean",
          description: "Generate a detailed validation report",
          default: true
        }
      },
      required: ["pluginPath"],
      additionalProperties: false
    }
  } as const;

  // REQUIRED: Description of what the tool does
  public readonly description =
    "Validates data-prepper plugin structure and compliance with the framework's architecture patterns, annotations, and integration requirements.";

  // REQUIRED: execute method that implements the tool's functionality
  public async execute(params: PluginValidatorParams) {
    const {
      pluginPath,
      validationType = "all",
      strictMode = false,
      generateReport = true
    } = params;

    try {
      // Validate plugin path exists
      const fullPluginPath = this.context.path.join(this.context.rootDir, pluginPath);
      if (!this.context.fs.existsSync(fullPluginPath)) {
        return {
          status: "error",
          message: `Plugin path does not exist: ${pluginPath}`,
          pluginPath
        };
      }

      const validation: any = {
        timestamp: new Date().toISOString(),
        pluginPath,
        validationType,
        strictMode,
        results: {
          overall: { passed: true, score: 100 },
          issues: [],
          warnings: [],
          suggestions: []
        }
      };

      if (validationType === "structure" || validationType === "all") {
        const structureResults = await this.validatePluginStructure(fullPluginPath);
        validation.results.structure = structureResults;
        this.updateOverallResults(validation.results, structureResults);
      }

      if (validationType === "annotations" || validationType === "all") {
        const annotationResults = await this.validateAnnotations(fullPluginPath);
        validation.results.annotations = annotationResults;
        this.updateOverallResults(validation.results, annotationResults);
      }

      if (validationType === "integration" || validationType === "all") {
        const integrationResults = await this.validateIntegration(fullPluginPath);
        validation.results.integration = integrationResults;
        this.updateOverallResults(validation.results, integrationResults);
      }

      if (strictMode) {
        const strictResults = await this.performStrictValidation(fullPluginPath);
        validation.results.strict = strictResults;
        this.updateOverallResults(validation.results, strictResults);
      }

      return {
        status: validation.results.overall.passed ? "success" : "warning",
        message: `Plugin validation completed with score: ${validation.results.overall.score}%`,
        validation: generateReport ? validation : this.summarizeValidation(validation)
      };

    } catch (err) {
      return {
        status: "error",
        message: "Failed to validate plugin",
        pluginPath,
        error: err instanceof Error ? err.message : String(err)
      };
    }
  }

  private async validatePluginStructure(pluginPath: string): Promise<any> {
    const results = {
      passed: true,
      score: 100,
      checks: [],
      issues: [],
      warnings: []
    };

    try {
      // Check for required directories
      const requiredDirs = ['src/main/java', 'src/test/java'];
      for (const dir of requiredDirs) {
        const dirPath = this.context.path.join(pluginPath, dir);
        if (this.context.fs.existsSync(dirPath)) {
          results.checks.push({ name: `${dir} directory exists`, passed: true });
        } else {
          results.checks.push({ name: `${dir} directory exists`, passed: false });
          results.issues.push(`Missing required directory: ${dir}`);
          results.score -= 15;
        }
      }

      // Check for build.gradle
      const buildGradle = this.context.path.join(pluginPath, 'build.gradle');
      if (this.context.fs.existsSync(buildGradle)) {
        results.checks.push({ name: 'build.gradle exists', passed: true });

        // Validate build.gradle content
        const buildContent = this.context.fs.readFileSync(buildGradle, 'utf8');
        if (buildContent.includes('data-prepper-plugin-framework')) {
          results.checks.push({ name: 'Uses data-prepper-plugin-framework', passed: true });
        } else {
          results.checks.push({ name: 'Uses data-prepper-plugin-framework', passed: false });
          results.warnings.push('Should depend on data-prepper-plugin-framework');
          results.score -= 5;
        }
      } else {
        results.checks.push({ name: 'build.gradle exists', passed: false });
        results.issues.push('Missing build.gradle file');
        results.score -= 20;
      }

      // Check for plugin class structure
      const srcPath = this.context.path.join(pluginPath, 'src/main/java');
      if (this.context.fs.existsSync(srcPath)) {
        const javaFiles = this.findJavaFiles(srcPath);
        const pluginClasses = javaFiles.filter(file => {
          const content = this.context.fs.readFileSync(file, 'utf8');
          return content.includes('@DataPrepperPlugin') || content.includes('implements Processor') ||
                 content.includes('implements Source') || content.includes('implements Sink');
        });

        if (pluginClasses.length > 0) {
          results.checks.push({ name: 'Plugin implementation found', passed: true });
        } else {
          results.checks.push({ name: 'Plugin implementation found', passed: false });
          results.issues.push('No plugin implementation classes found');
          results.score -= 25;
        }
      }

      results.passed = results.score >= 70;
      return results;

    } catch (error) {
      results.passed = false;
      results.issues.push(`Structure validation failed: ${error}`);
      return results;
    }
  }

  private async validateAnnotations(pluginPath: string): Promise<any> {
    const results = {
      passed: true,
      score: 100,
      checks: [],
      issues: [],
      warnings: []
    };

    try {
      const srcPath = this.context.path.join(pluginPath, 'src/main/java');
      if (!this.context.fs.existsSync(srcPath)) {
        results.passed = false;
        results.issues.push('No source directory found');
        return results;
      }

      const javaFiles = this.findJavaFiles(srcPath);
      let foundPluginAnnotation = false;
      let foundConfigurationAnnotation = false;

      for (const file of javaFiles) {
        const content = this.context.fs.readFileSync(file, 'utf8');
        const fileName = this.context.path.basename(file);

        // Check for @DataPrepperPlugin annotation
        if (content.includes('@DataPrepperPlugin')) {
          foundPluginAnnotation = true;
          results.checks.push({ name: `@DataPrepperPlugin found in ${fileName}`, passed: true });

          // Validate required attributes
          const pluginMatch = content.match(/@DataPrepperPlugin\s*\(\s*([^)]+)\s*\)/s);
          if (pluginMatch) {
            const attributes = pluginMatch[1];
            if (!attributes.includes('name')) {
              results.warnings.push(`${fileName}: @DataPrepperPlugin should specify 'name' attribute`);
              results.score -= 5;
            }
            if (!attributes.includes('pluginType')) {
              results.warnings.push(`${fileName}: @DataPrepperPlugin should specify 'pluginType' attribute`);
              results.score -= 5;
            }
          }
        }

        // Check for configuration classes
        if (content.includes('@JsonProperty') || content.includes('@JsonClassDescription')) {
          foundConfigurationAnnotation = true;
          results.checks.push({ name: `Configuration annotations found in ${fileName}`, passed: true });
        }

        // Check for proper plugin interface implementation
        if (content.includes('implements Processor') || content.includes('implements Source') || content.includes('implements Sink')) {
          results.checks.push({ name: `Plugin interface implementation in ${fileName}`, passed: true });
        }
      }

      if (!foundPluginAnnotation) {
        results.issues.push('No @DataPrepperPlugin annotation found');
        results.score -= 30;
        results.passed = false;
      }

      if (!foundConfigurationAnnotation) {
        results.warnings.push('No configuration annotations found - consider adding @JsonProperty annotations');
        results.score -= 10;
      }

      return results;

    } catch (error) {
      results.passed = false;
      results.issues.push(`Annotation validation failed: ${error}`);
      return results;
    }
  }

  private async validateIntegration(pluginPath: string): Promise<any> {
    const results = {
      passed: true,
      score: 100,
      checks: [],
      issues: [],
      warnings: []
    };

    try {
      // Check for integration tests
      const integrationTestPath = this.context.path.join(pluginPath, 'src/integrationTest/java');
      if (this.context.fs.existsSync(integrationTestPath)) {
        results.checks.push({ name: 'Integration tests present', passed: true });
      } else {
        results.warnings.push('No integration tests found - consider adding integration tests');
        results.score -= 15;
      }

      // Check for unit tests
      const testPath = this.context.path.join(pluginPath, 'src/test/java');
      if (this.context.fs.existsSync(testPath)) {
        const testFiles = this.findJavaFiles(testPath);
        if (testFiles.length > 0) {
          results.checks.push({ name: 'Unit tests present', passed: true });

          // Check for test quality
          let foundProperTests = false;
          for (const testFile of testFiles) {
            const content = this.context.fs.readFileSync(testFile, 'utf8');
            if (content.includes('@Test') && content.includes('@ExtendWith(MockitoExtension.class)')) {
              foundProperTests = true;
              break;
            }
          }

          if (foundProperTests) {
            results.checks.push({ name: 'Proper test structure found', passed: true });
          } else {
            results.warnings.push('Tests should use @Test and @ExtendWith(MockitoExtension.class)');
            results.score -= 10;
          }
        } else {
          results.warnings.push('Test directory exists but no test files found');
          results.score -= 20;
        }
      } else {
        results.issues.push('No unit tests found');
        results.score -= 25;
      }

      // Check for plugin service registration
      const servicesPath = this.context.path.join(pluginPath, 'src/main/resources/META-INF/services');
      if (this.context.fs.existsSync(servicesPath)) {
        results.checks.push({ name: 'Service registration directory present', passed: true });
      } else {
        results.warnings.push('No META-INF/services directory - plugin may not be auto-discovered');
        results.score -= 10;
      }

      results.passed = results.score >= 70;
      return results;

    } catch (error) {
      results.passed = false;
      results.issues.push(`Integration validation failed: ${error}`);
      return results;
    }
  }

  private async performStrictValidation(pluginPath: string): Promise<any> {
    const results = {
      passed: true,
      score: 100,
      checks: [],
      issues: [],
      warnings: []
    };

    try {
      // Check for Javadoc documentation
      const srcPath = this.context.path.join(pluginPath, 'src/main/java');
      if (this.context.fs.existsSync(srcPath)) {
        const javaFiles = this.findJavaFiles(srcPath);
        let documentedClasses = 0;

        for (const file of javaFiles) {
          const content = this.context.fs.readFileSync(file, 'utf8');
          if (content.includes('/**') && content.includes('@since')) {
            documentedClasses++;
          }
        }

        const documentationRatio = javaFiles.length > 0 ? documentedClasses / javaFiles.length : 0;
        if (documentationRatio >= 0.8) {
          results.checks.push({ name: 'Good Javadoc coverage', passed: true });
        } else {
          results.warnings.push(`Only ${Math.round(documentationRatio * 100)}% of classes have proper Javadoc`);
          results.score -= 15;
        }
      }

      // Check for configuration validation
      const configFiles = this.findJavaFiles(this.context.path.join(pluginPath, 'src/main/java'))
        .filter(file => {
          const content = this.context.fs.readFileSync(file, 'utf8');
          return content.includes('Config') && (content.includes('@JsonProperty') || content.includes('@Valid'));
        });

      if (configFiles.length > 0) {
        results.checks.push({ name: 'Configuration classes found', passed: true });
      } else {
        results.warnings.push('No configuration validation classes found');
        results.score -= 10;
      }

      results.passed = results.score >= 80; // Stricter threshold for strict mode
      return results;

    } catch (error) {
      results.passed = false;
      results.issues.push(`Strict validation failed: ${error}`);
      return results;
    }
  }

  private findJavaFiles(directory: string): string[] {
    const files: string[] = [];
    try {
      const walkDir = (dir: string) => {
        const items = this.context.fs.readdirSync(dir);
        for (const item of items) {
          const fullPath = this.context.path.join(dir, item);
          const stat = this.context.fs.statSync(fullPath);
          if (stat.isDirectory()) {
            walkDir(fullPath);
          } else if (item.endsWith('.java')) {
            files.push(fullPath);
          }
        }
      };
      walkDir(directory);
    } catch (error) {
      // Ignore errors
    }
    return files;
  }

  private updateOverallResults(overall: any, sectionResults: any): void {
    if (!sectionResults.passed) {
      overall.overall.passed = false;
    }

    if (sectionResults.issues) {
      overall.issues.push(...sectionResults.issues);
    }

    if (sectionResults.warnings) {
      overall.warnings.push(...sectionResults.warnings);
    }

    // Calculate weighted average score
    const currentScore = overall.overall.score;
    const newScore = sectionResults.score || 100;
    overall.overall.score = Math.round((currentScore + newScore) / 2);
  }

  private summarizeValidation(validation: any): any {
    return {
      timestamp: validation.timestamp,
      pluginPath: validation.pluginPath,
      overall: validation.results.overall,
      summary: {
        totalIssues: validation.results.issues.length,
        totalWarnings: validation.results.warnings.length,
        validationType: validation.validationType,
        strictMode: validation.strictMode
      },
      recommendations: this.generateRecommendations(validation.results)
    };
  }

  private generateRecommendations(results: any): string[] {
    const recommendations: string[] = [];

    if (results.issues.length > 0) {
      recommendations.push(`Address ${results.issues.length} critical issues found during validation`);
    }

    if (results.warnings.length > 0) {
      recommendations.push(`Consider addressing ${results.warnings.length} warnings to improve plugin quality`);
    }

    if (results.overall.score < 90) {
      recommendations.push('Plugin validation score is below 90% - review and address identified issues');
    }

    return recommendations;
  }
}

// REQUIRED: Default export must be the tool class
export default PluginValidator;
