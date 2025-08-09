import NetworkRequest from '@Services/Network/NetworkRequest';
import AsyncStorage from '@react-native-async-storage/async-storage';
import { AsyncStorageKeys } from '../../utils/constants';

// Game event type definition
export interface GameEvent {
  event_type: string;
  timestamp: number;
  user_id: string;
  session_id: string;
  device_info: DeviceInfo;
  location?: LocationData;
  metadata?: Record<string, any>;
}

interface LocationData {
  latitude: number;
  longitude: number;
  accuracy: number;
  speed?: number;
  altitude?: number;
  heading?: number;
}

interface DeviceInfo {
  device_id: string;
  platform: 'ios' | 'android';
  app_version: string;
  os_version: string;
  network_type?: string;
}

class AnalyticsService {
  private eventQueue: GameEvent[] = [];
  private sessionId: string = '';
  private flushInterval: NodeJS.Timeout | null = null;
  private readonly BATCH_SIZE = 50;
  private readonly FLUSH_INTERVAL = 10000; // 10 seconds
  private readonly MAX_QUEUE_SIZE = 500;

  constructor() {
    this.initializeSession();
    this.startBatchProcessing();
  }

  private async initializeSession() {
    this.sessionId = Date.now().toString() + Math.random().toString(36);
  }

  private startBatchProcessing() {
    this.flushInterval = setInterval(() => {
      this.flushEvents();
    }, this.FLUSH_INTERVAL);
  }

  /**
   * Record location exploration events
   */
  public async trackLocationExploration(locationData: LocationData, tileData: any) {
    const event: GameEvent = {
      event_type: 'location_exploration',
      timestamp: Date.now(),
      user_id: await this.getUserId(),
      session_id: this.sessionId,
      device_info: await this.getDeviceInfo(),
      location: locationData,
      metadata: {
        tile_x: tileData.tileX,
        tile_y: tileData.tileY,
        tile_z: tileData.tileZ,
        event_type: tileData.eventType,
        exploration_radius: 100, // meters
        movement_speed: locationData.speed || 0,
      }
    };

    this.addEvent(event);
  }

  /**
   * Record user behavior events
   */
  public async trackUserAction(actionType: string, metadata?: Record<string, any>) {
    const event: GameEvent = {
      event_type: 'user_action',
      timestamp: Date.now(),
      user_id: await this.getUserId(),
      session_id: this.sessionId,
      device_info: await this.getDeviceInfo(),
      metadata: {
        action_type: actionType,
        ...metadata
      }
    };

    this.addEvent(event);
  }

  /**
   * Record suspicious behavior (for anti-cheat)
   */
  public async trackSuspiciousActivity(suspiciousType: string, location: LocationData, details: any) {
    const event: GameEvent = {
      event_type: 'suspicious_activity',
      timestamp: Date.now(),
      user_id: await this.getUserId(),
      session_id: this.sessionId,
      device_info: await this.getDeviceInfo(),
      location: location,
      metadata: {
        suspicious_type: suspiciousType,
        details: details,
        risk_score: this.calculateRiskScore(suspiciousType, details),
      }
    };

    // Send suspicious events immediately
    this.sendImmediateEvent(event);
  }

  /**
   * Record application lifecycle events
   */
  public async trackAppLifecycle(lifecycleEvent: string, metadata?: Record<string, any>) {
    const event: GameEvent = {
      event_type: 'app_lifecycle',
      timestamp: Date.now(),
      user_id: await this.getUserId(),
      session_id: this.sessionId,
      device_info: await this.getDeviceInfo(),
      metadata: {
        lifecycle_event: lifecycleEvent,
        ...metadata
      }
    };

    this.addEvent(event);
  }

  private addEvent(event: GameEvent) {
    this.eventQueue.push(event);
    
    // Prevent queue from becoming too large
    if (this.eventQueue.length > this.MAX_QUEUE_SIZE) {
      this.eventQueue.splice(0, this.BATCH_SIZE);
    }

    // Send immediately when queue reaches batch size
    if (this.eventQueue.length >= this.BATCH_SIZE) {
      this.flushEvents();
    }
  }

  private async flushEvents() {
    if (this.eventQueue.length === 0) return;

    const eventsToSend = this.eventQueue.splice(0, this.BATCH_SIZE);
    
    try {
      await this.sendEventsBatch(eventsToSend);
    } catch (error) {
      console.error('Failed to send events:', error);
      // Re-add failed events to queue (max 3 retries)
      eventsToSend.forEach(event => {
        if (!event.metadata?.retry_count || event.metadata.retry_count < 3) {
          event.metadata = { ...event.metadata, retry_count: (event.metadata?.retry_count || 0) + 1 };
          this.eventQueue.unshift(event);
        }
      });
    }
  }

  private async sendEventsBatch(events: GameEvent[]) {
    const request = await new NetworkRequest('/analytics/events').prepare({
      method: 'POST',
      data: {
        events: events,
        batch_id: Date.now().toString(),
        app_version: await this.getAppVersion(),
      },
      needEncrypt: true,
    });

    return await request.execute();
  }

  private async sendImmediateEvent(event: GameEvent) {
    try {
      const request = await new NetworkRequest('/analytics/realtime-event').prepare({
        method: 'POST',
        data: event,
        needEncrypt: true,
      });

      await request.execute();
    } catch (error) {
      console.error('Failed to send immediate event:', error);
      // Add failed urgent events to queue as well
      this.addEvent(event);
    }
  }

  private calculateRiskScore(suspiciousType: string, details: any): number {
    // Simple risk scoring algorithm
    let score = 0;
    
    switch (suspiciousType) {
      case 'location_spoofing':
        score = details.impossibleSpeed ? 80 : 60;
        break;
      case 'rapid_exploration':
        score = details.tilesPerMinute > 10 ? 90 : 70;
        break;
      case 'automation_detected':
        score = 95;
        break;
      default:
        score = 50;
    }

    return Math.min(score, 100);
  }

  private async getUserId(): Promise<string> {
    // Get user ID from AsyncStorage or Redux store
    const userInfo = await AsyncStorage.getItem(AsyncStorageKeys.USER_INFO);
    return userInfo ? JSON.parse(userInfo).uid : 'anonymous';
  }

  private async getDeviceInfo(): Promise<DeviceInfo> {
    const deviceId = await AsyncStorage.getItem(AsyncStorageKeys.DEVICE_ID);
    
    return {
      device_id: deviceId || 'unknown',
      platform: Platform.OS as 'ios' | 'android',
      app_version: VersionInfo.appVersion,
      os_version: Platform.Version.toString(),
      // network_type: await NetInfo.fetch().then(state => state.type),
    };
  }

  private async getAppVersion(): Promise<string> {
    return VersionInfo.appVersion;
  }

  public destroy() {
    if (this.flushInterval) {
      clearInterval(this.flushInterval);
    }
    // Send remaining events
    this.flushEvents();
  }
}

export default new AnalyticsService();