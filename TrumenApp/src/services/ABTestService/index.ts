/**
 * A/B Testing Service for TrumenApp
 * Manages feature flags, experiment assignments, and conversion tracking
 */

import AsyncStorage from '@react-native-async-storage/async-storage';
import NetworkRequest from '@Services/Network/NetworkRequest';
import AnalyticsService from '@Services/AnalyticsService';
import { AsyncStorageKeys } from '../../utils/constants';

export interface ABTestConfig {
  test_name: string;
  test_id: string;
  variants: ABTestVariant[];
  targeting_rules?: TargetingRule[];
  traffic_allocation: number; // 0-1, percentage of users to include
  start_date: string;
  end_date: string;
  status: 'active' | 'paused' | 'completed';
  primary_metric: string;
  secondary_metrics?: string[];
}

export interface ABTestVariant {
  variant_id: string;
  variant_name: string;
  traffic_weight: number; // 0-1, percentage of test traffic
  config: Record<string, any>; // Feature configuration
}

export interface TargetingRule {
  attribute: string;
  operator: 'equals' | 'not_equals' | 'contains' | 'greater_than' | 'less_than';
  value: any;
}

export interface UserAssignment {
  test_name: string;
  variant_id: string;
  variant_name: string;
  assignment_timestamp: number;
  config: Record<string, any>;
}

export interface ConversionEvent {
  test_name: string;
  variant_id: string;
  metric_name: string;
  value: number;
  timestamp: number;
  user_id: string;
}

class ABTestService {
  private userAssignments: Map<string, UserAssignment> = new Map();
  private activeTests: Map<string, ABTestConfig> = new Map();
  private userAttributes: Record<string, any> = {};
  private initialized: boolean = false;

  /**
   * Initialize the A/B testing service
   */
  public async initialize(): Promise<void> {
    try {
      // Load user assignments from storage
      await this.loadUserAssignments();
      
      // Load user attributes
      await this.loadUserAttributes();
      
      // Fetch active experiments from server
      await this.fetchActiveExperiments();
      
      this.initialized = true;
      
      // Track initialization event
      AnalyticsService.trackUserAction('ab_test_service_initialized', {
        active_tests_count: this.activeTests.size,
        user_assignments_count: this.userAssignments.size,
      });
      
    } catch (error) {
      console.error('Failed to initialize A/B testing service:', error);
    }
  }

  /**
   * Get variant for a specific test
   */
  public getVariant(testName: string): UserAssignment | null {
    if (!this.initialized) {
      console.warn('A/B testing service not initialized');
      return null;
    }

    // Check if user already has assignment
    const existingAssignment = this.userAssignments.get(testName);
    if (existingAssignment) {
      return existingAssignment;
    }

    // Get test configuration
    const testConfig = this.activeTests.get(testName);
    if (!testConfig) {
      console.warn(`Test ${testName} not found or not active`);
      return null;
    }

    // Check if test is active
    const now = new Date().getTime();
    const startTime = new Date(testConfig.start_date).getTime();
    const endTime = new Date(testConfig.end_date).getTime();
    
    if (now < startTime || now > endTime || testConfig.status !== 'active') {
      return null;
    }

    // Check targeting rules
    if (!this.matchesTargetingRules(testConfig.targeting_rules)) {
      return null;
    }

    // Check traffic allocation
    const userId = this.getUserId();
    const userHash = this.hashUserId(userId, testName);
    const trafficAllocation = userHash % 100;
    
    if (trafficAllocation >= testConfig.traffic_allocation * 100) {
      // User not in experiment traffic
      return null;
    }

    // Assign variant based on traffic weights
    const variant = this.assignVariant(testConfig.variants, userHash);
    if (!variant) {
      return null;
    }

    // Create assignment
    const assignment: UserAssignment = {
      test_name: testName,
      variant_id: variant.variant_id,
      variant_name: variant.variant_name,
      assignment_timestamp: now,
      config: variant.config,
    };

    // Store assignment
    this.userAssignments.set(testName, assignment);
    this.saveUserAssignments();

    // Track assignment event
    AnalyticsService.trackUserAction('ab_test_assignment', {
      test_name: testName,
      variant_id: variant.variant_id,
      variant_name: variant.variant_name,
      assignment_timestamp: now,
    });

    return assignment;
  }

  /**
   * Check if user is in a specific test variant
   */
  public isInVariant(testName: string, variantName: string): boolean {
    const assignment = this.getVariant(testName);
    return assignment?.variant_name === variantName;
  }

  /**
   * Get configuration value for a test
   */
  public getConfig<T>(testName: string, configKey: string, defaultValue: T): T {
    const assignment = this.getVariant(testName);
    if (!assignment || !assignment.config) {
      return defaultValue;
    }
    
    return assignment.config[configKey] ?? defaultValue;
  }

  /**
   * Track conversion event
   */
  public trackConversion(testName: string, metricName: string, value: number = 1): void {
    const assignment = this.userAssignments.get(testName);
    if (!assignment) {
      console.warn(`No assignment found for test ${testName} when tracking conversion`);
      return;
    }

    const conversionEvent: ConversionEvent = {
      test_name: testName,
      variant_id: assignment.variant_id,
      metric_name: metricName,
      value: value,
      timestamp: Date.now(),
      user_id: this.getUserId(),
    };

    // Send conversion event immediately
    this.sendConversionEvent(conversionEvent);

    // Track in analytics service
    AnalyticsService.trackUserAction('ab_test_conversion', {
      test_name: testName,
      variant_id: assignment.variant_id,
      variant_name: assignment.variant_name,
      metric_name: metricName,
      value: value,
    });
  }

  /**
   * Force refresh experiments from server
   */
  public async refreshExperiments(): Promise<void> {
    try {
      await this.fetchActiveExperiments();
    } catch (error) {
      console.error('Failed to refresh experiments:', error);
    }
  }

  private async loadUserAssignments(): Promise<void> {
    try {
      const stored = await AsyncStorage.getItem(AsyncStorageKeys.AB_TEST_ASSIGNMENTS);
      if (stored) {
        const assignments = JSON.parse(stored);
        for (const [testName, assignment] of Object.entries(assignments)) {
          this.userAssignments.set(testName, assignment as UserAssignment);
        }
      }
    } catch (error) {
      console.error('Failed to load user assignments:', error);
    }
  }

  private async saveUserAssignments(): Promise<void> {
    try {
      const assignments: Record<string, UserAssignment> = {};
      for (const [testName, assignment] of this.userAssignments) {
        assignments[testName] = assignment;
      }
      await AsyncStorage.setItem(
        AsyncStorageKeys.AB_TEST_ASSIGNMENTS,
        JSON.stringify(assignments)
      );
    } catch (error) {
      console.error('Failed to save user assignments:', error);
    }
  }

  private async loadUserAttributes(): Promise<void> {
    try {
      // Load user profile information
      const userInfo = await AsyncStorage.getItem(AsyncStorageKeys.USER_INFO);
      const deviceInfo = await AsyncStorage.getItem(AsyncStorageKeys.DEVICE_INFO);
      
      if (userInfo) {
        const user = JSON.parse(userInfo);
        this.userAttributes = {
          user_id: user.uid,
          level: user.level || 0,
          registration_date: user.created_at,
          platform: Platform.OS,
          app_version: VersionInfo.appVersion,
          ...this.userAttributes,
        };
      }

      if (deviceInfo) {
        const device = JSON.parse(deviceInfo);
        this.userAttributes = {
          ...this.userAttributes,
          device_id: device.device_id,
          device_model: device.model,
        };
      }
    } catch (error) {
      console.error('Failed to load user attributes:', error);
    }
  }

  private async fetchActiveExperiments(): Promise<void> {
    try {
      const request = await new NetworkRequest('/ab-tests/active').prepare({
        method: 'GET',
      });

      const response = await request.execute();
      
      if (response.experiments) {
        this.activeTests.clear();
        for (const experiment of response.experiments) {
          this.activeTests.set(experiment.test_name, experiment);
        }
      }
    } catch (error) {
      console.error('Failed to fetch active experiments:', error);
    }
  }

  private matchesTargetingRules(rules?: TargetingRule[]): boolean {
    if (!rules || rules.length === 0) {
      return true; // No targeting rules, everyone qualifies
    }

    return rules.every(rule => {
      const userValue = this.userAttributes[rule.attribute];
      
      switch (rule.operator) {
        case 'equals':
          return userValue === rule.value;
        case 'not_equals':
          return userValue !== rule.value;
        case 'contains':
          return typeof userValue === 'string' && userValue.includes(rule.value);
        case 'greater_than':
          return typeof userValue === 'number' && userValue > rule.value;
        case 'less_than':
          return typeof userValue === 'number' && userValue < rule.value;
        default:
          return false;
      }
    });
  }

  private assignVariant(variants: ABTestVariant[], userHash: number): ABTestVariant | null {
    const hashBucket = userHash % 1000; // Use 0-999 for more precision
    let cumulativeWeight = 0;

    for (const variant of variants) {
      cumulativeWeight += variant.traffic_weight * 1000;
      if (hashBucket < cumulativeWeight) {
        return variant;
      }
    }

    return null; // Should not happen if weights sum to 1.0
  }

  private hashUserId(userId: string, testName: string): number {
    // Simple hash function for consistent bucketing
    const str = `${userId}_${testName}`;
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      const char = str.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash; // Convert to 32-bit integer
    }
    return Math.abs(hash);
  }

  private getUserId(): string {
    return this.userAttributes.user_id || 'anonymous';
  }

  private async sendConversionEvent(event: ConversionEvent): Promise<void> {
    try {
      const request = await new NetworkRequest('/ab-tests/conversion').prepare({
        method: 'POST',
        data: event,
        needEncrypt: true,
      });

      await request.execute();
    } catch (error) {
      console.error('Failed to send conversion event:', error);
    }
  }

  /**
   * Get all active assignments for debugging
   */
  public getActiveAssignments(): Record<string, UserAssignment> {
    const assignments: Record<string, UserAssignment> = {};
    for (const [testName, assignment] of this.userAssignments) {
      assignments[testName] = assignment;
    }
    return assignments;
  }

  /**
   * Clear all assignments (for testing purposes)
   */
  public async clearAssignments(): Promise<void> {
    this.userAssignments.clear();
    await AsyncStorage.removeItem(AsyncStorageKeys.AB_TEST_ASSIGNMENTS);
  }
}

// Singleton instance
export default new ABTestService();

// Convenience hook for React components
export const useABTest = (testName: string) => {
  const getVariant = () => ABTestService.getVariant(testName);
  const isInVariant = (variantName: string) => ABTestService.isInVariant(testName, variantName);
  const getConfig = <T>(configKey: string, defaultValue: T) => 
    ABTestService.getConfig(testName, configKey, defaultValue);
  const trackConversion = (metricName: string, value?: number) => 
    ABTestService.trackConversion(testName, metricName, value);

  return {
    variant: getVariant(),
    isInVariant,
    getConfig,
    trackConversion,
  };
};