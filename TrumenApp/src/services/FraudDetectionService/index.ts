import AnalyticsService from '@Services/AnalyticsService';
import { haversine } from '../../utils/coordinate';

interface LocationPoint {
  latitude: number;
  longitude: number;
  timestamp: number;
  accuracy: number;
  speed?: number;
}

interface SuspiciousActivityResult {
  isSuspicious: boolean;
  reason: string;
  riskScore: number;
  details: any;
}

class FraudDetectionService {
  private locationHistory: LocationPoint[] = [];
  private readonly MAX_HISTORY = 100;
  private readonly MAX_HUMAN_SPEED = 50; // km/h - Maximum human movement speed
  private readonly MIN_LOCATION_ACCURACY = 100; // meters - Minimum location accuracy requirement
  private readonly RAPID_EXPLORATION_THRESHOLD = 8; // Tiles per minute threshold

  private tileExplorationHistory: Array<{tileId: string, timestamp: number}> = [];

  /**
   * Check if location data is suspicious
   */
  public checkLocationValidity(newLocation: LocationPoint): SuspiciousActivityResult {
    // Add to history record
    this.addLocationToHistory(newLocation);

    // Multiple checks
    const speedCheck = this.checkImpossibleSpeed(newLocation);
    const accuracyCheck = this.checkLocationAccuracy(newLocation);
    const patternCheck = this.checkMovementPattern(newLocation);
    const teleportCheck = this.checkTeleportation(newLocation);

    // Comprehensive assessment
    const suspiciousChecks = [speedCheck, accuracyCheck, patternCheck, teleportCheck].filter(
      check => check.isSuspicious
    );

    if (suspiciousChecks.length > 0) {
      const maxRiskScore = Math.max(...suspiciousChecks.map(check => check.riskScore));
      const primaryReason = suspiciousChecks.find(check => check.riskScore === maxRiskScore)?.reason || 'unknown';

      // Record suspicious activity
      AnalyticsService.trackSuspiciousActivity('location_spoofing', {
        latitude: newLocation.latitude,
        longitude: newLocation.longitude,
        accuracy: newLocation.accuracy,
        speed: newLocation.speed,
      }, {
        failed_checks: suspiciousChecks.map(check => check.reason),
        location_history_count: this.locationHistory.length,
        primary_reason: primaryReason,
      });

      return {
        isSuspicious: true,
        reason: primaryReason,
        riskScore: maxRiskScore,
        details: suspiciousChecks,
      };
    }

    return {
      isSuspicious: false,
      reason: 'location_valid',
      riskScore: 0,
      details: null,
    };
  }

  /**
   * Check if exploration speed is abnormal
   */
  public checkExplorationRate(tileId: string): SuspiciousActivityResult {
    const now = Date.now();
    this.tileExplorationHistory.push({ tileId, timestamp: now });

    // Keep records from the last 1 hour
    const oneHourAgo = now - (60 * 60 * 1000);
    this.tileExplorationHistory = this.tileExplorationHistory.filter(
      record => record.timestamp > oneHourAgo
    );

    // Calculate exploration speed in the last 1 minute
    const oneMinuteAgo = now - (60 * 1000);
    const recentExplorations = this.tileExplorationHistory.filter(
      record => record.timestamp > oneMinuteAgo
    );

    if (recentExplorations.length > this.RAPID_EXPLORATION_THRESHOLD) {
      AnalyticsService.trackSuspiciousActivity('rapid_exploration', {
        latitude: 0, // Will be provided by caller
        longitude: 0,
        accuracy: 0,
      }, {
        tiles_per_minute: recentExplorations.length,
        threshold: this.RAPID_EXPLORATION_THRESHOLD,
        recent_tiles: recentExplorations.map(r => r.tileId),
      });

      return {
        isSuspicious: true,
        reason: 'rapid_exploration',
        riskScore: 85,
        details: {
          tilesPerMinute: recentExplorations.length,
          threshold: this.RAPID_EXPLORATION_THRESHOLD,
        },
      };
    }

    return {
      isSuspicious: false,
      reason: 'exploration_rate_normal',
      riskScore: 0,
      details: null,
    };
  }

  /**
   * Detect automated behavior patterns
   */
  public checkAutomationPattern(): SuspiciousActivityResult {
    if (this.locationHistory.length < 10) {
      return { isSuspicious: false, reason: 'insufficient_data', riskScore: 0, details: null };
    }

    const patterns = this.analyzeMovementPatterns();
    
    // Check for obvious automation characteristics
    if (patterns.perfectTiming || patterns.unnaturalPrecision || patterns.repetitiveMovement) {
      return {
        isSuspicious: true,
        reason: 'automation_detected',
        riskScore: 95,
        details: patterns,
      };
    }

    return {
      isSuspicious: false,
      reason: 'human_behavior',
      riskScore: 0,
      details: patterns,
    };
  }

  private checkImpossibleSpeed(newLocation: LocationPoint): SuspiciousActivityResult {
    if (this.locationHistory.length === 0) {
      return { isSuspicious: false, reason: 'no_previous_location', riskScore: 0, details: null };
    }

    const lastLocation = this.locationHistory[this.locationHistory.length - 1];
    const distance = haversine(
      lastLocation.latitude, lastLocation.longitude,
      newLocation.latitude, newLocation.longitude
    );
    
    const timeDiff = (newLocation.timestamp - lastLocation.timestamp) / 1000; // seconds
    const speed = (distance * 1000) / timeDiff * 3.6; // km/h

    if (speed > this.MAX_HUMAN_SPEED && timeDiff > 5) { // Ignore rapid updates within 5 seconds
      return {
        isSuspicious: true,
        reason: 'impossible_speed',
        riskScore: Math.min(90, 50 + (speed - this.MAX_HUMAN_SPEED)),
        details: {
          calculated_speed: speed,
          max_allowed_speed: this.MAX_HUMAN_SPEED,
          distance: distance,
          time_diff: timeDiff,
        },
      };
    }

    return { isSuspicious: false, reason: 'speed_normal', riskScore: 0, details: null };
  }

  private checkLocationAccuracy(location: LocationPoint): SuspiciousActivityResult {
    if (location.accuracy > this.MIN_LOCATION_ACCURACY) {
      return {
        isSuspicious: true,
        reason: 'poor_accuracy',
        riskScore: 30,
        details: {
          accuracy: location.accuracy,
          required_accuracy: this.MIN_LOCATION_ACCURACY,
        },
      };
    }

    return { isSuspicious: false, reason: 'accuracy_good', riskScore: 0, details: null };
  }

  private checkMovementPattern(newLocation: LocationPoint): SuspiciousActivityResult {
    if (this.locationHistory.length < 5) {
      return { isSuspicious: false, reason: 'insufficient_pattern_data', riskScore: 0, details: null };
    }

    // Check if appearing repeatedly at exactly the same point (common GPS spoofing feature)
    const duplicates = this.locationHistory.filter(loc => 
      Math.abs(loc.latitude - newLocation.latitude) < 0.000001 &&
      Math.abs(loc.longitude - newLocation.longitude) < 0.000001
    );

    if (duplicates.length > 3) {
      return {
        isSuspicious: true,
        reason: 'identical_coordinates',
        riskScore: 70,
        details: {
          duplicate_count: duplicates.length,
          coordinate: `${newLocation.latitude},${newLocation.longitude}`,
        },
      };
    }

    return { isSuspicious: false, reason: 'pattern_normal', riskScore: 0, details: null };
  }

  private checkTeleportation(newLocation: LocationPoint): SuspiciousActivityResult {
    if (this.locationHistory.length === 0) {
      return { isSuspicious: false, reason: 'no_previous_location', riskScore: 0, details: null };
    }

    const lastLocation = this.locationHistory[this.locationHistory.length - 1];
    const distance = haversine(
      lastLocation.latitude, lastLocation.longitude,
      newLocation.latitude, newLocation.longitude
    );
    
    const timeDiff = (newLocation.timestamp - lastLocation.timestamp) / 1000; // seconds

    // Check if moved a long distance in a short time (teleportation)
    if (distance > 1 && timeDiff < 2) { // Move more than 1km within 2 seconds
      return {
        isSuspicious: true,
        reason: 'teleportation',
        riskScore: 95,
        details: {
          distance: distance,
          time_diff: timeDiff,
          from: `${lastLocation.latitude},${lastLocation.longitude}`,
          to: `${newLocation.latitude},${newLocation.longitude}`,
        },
      };
    }

    return { isSuspicious: false, reason: 'movement_normal', riskScore: 0, details: null };
  }

  private analyzeMovementPatterns() {
    const recentLocations = this.locationHistory.slice(-20); // Analyze the last 20 locations
    
    // Check if time intervals are too regular (robot behavior characteristics)
    const timeIntervals = [];
    for (let i = 1; i < recentLocations.length; i++) {
      timeIntervals.push(recentLocations[i].timestamp - recentLocations[i-1].timestamp);
    }
    
    const avgInterval = timeIntervals.reduce((a, b) => a + b, 0) / timeIntervals.length;
    const intervalVariance = timeIntervals.reduce((sum, interval) => {
      return sum + Math.pow(interval - avgInterval, 2);
    }, 0) / timeIntervals.length;
    
    // Check if movement distances are too regular
    const distances = [];
    for (let i = 1; i < recentLocations.length; i++) {
      const distance = haversine(
        recentLocations[i-1].latitude, recentLocations[i-1].longitude,
        recentLocations[i].latitude, recentLocations[i].longitude
      );
      distances.push(distance);
    }

    return {
      perfectTiming: intervalVariance < 100, // Time interval variation too small
      unnaturalPrecision: distances.every(d => d > 0 && d < 0.001), // Movement distance too precise
      repetitiveMovement: this.detectRepetitivePattern(recentLocations),
      averageInterval: avgInterval,
      intervalVariance: intervalVariance,
    };
  }

  private detectRepetitivePattern(locations: LocationPoint[]): boolean {
    // Simple repetitive pattern detection
    if (locations.length < 6) return false;

    const pattern = locations.slice(0, 3);
    const nextPattern = locations.slice(3, 6);

    return pattern.every((loc, i) => {
      const nextLoc = nextPattern[i];
      return nextLoc && 
        Math.abs(loc.latitude - nextLoc.latitude) < 0.0001 &&
        Math.abs(loc.longitude - nextLoc.longitude) < 0.0001;
    });
  }

  private addLocationToHistory(location: LocationPoint) {
    this.locationHistory.push(location);
    
    // Keep history records within limits
    if (this.locationHistory.length > this.MAX_HISTORY) {
      this.locationHistory.shift();
    }
  }

  public getLocationHistory(): LocationPoint[] {
    return [...this.locationHistory];
  }

  public clearHistory() {
    this.locationHistory = [];
    this.tileExplorationHistory = [];
  }
}

export default new FraudDetectionService();