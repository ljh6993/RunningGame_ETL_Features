import {useRealm} from '@realm/react';
import Realm from 'realm';
import {DEFAULT_TILE_SIZE, MIN_ZOOM_LEVEL} from '../utils/constants';
import {sendTileRequest} from '@Services/ValidateService';
import AnalyticsService from '@Services/AnalyticsService';
import FraudDetectionService from '@Services/FraudDetectionService';

const MAX_ZOOM_LEVEL = 17; // Because native zoom level doesn't match RN side, use different zoom level for calculations

const Fix_number = 10;
const TILE_SIZE = DEFAULT_TILE_SIZE;

export const useLocalDB = () => {
  const realm = useRealm();
  // private _MapX;
  // private _MapY;
  // private _FULL;

  // Return all user points
  const getLatLngArr = () => {
    const array = realm.objects('UserLocationModel');

    return array;
  };

  const deleteAllLocations = () => {
    realm.write(() => {
      realm.deleteAll();
    });
  };

  const getLocationCount = () => {
    const locCount = realm.objects('UserLocationModel').length;
    return locCount;
  };

  const recordLocationData = (lat: number, lng: number, accuracy?: number, speed?: number) => {
    // Create location point data for anti-cheat detection
    const locationPoint = {
      latitude: lat,
      longitude: lng,
      timestamp: Date.now(),
      accuracy: accuracy || 50, // Default accuracy 50 meters
      speed: speed,
    };

    // Execute anti-cheat detection
    const fraudCheck = FraudDetectionService.checkLocationValidity(locationPoint);
    if (fraudCheck.isSuspicious && fraudCheck.riskScore > 80) {
      console.warn('Suspicious location detected:', fraudCheck.reason);
      // In high-risk situations, can choose not to record or mark data
      AnalyticsService.trackUserAction('location_rejected', {
        reason: fraudCheck.reason,
        risk_score: fraudCheck.riskScore,
        latitude: lat,
        longitude: lng,
      });
      return; // Reject recording suspicious location
    }

    const {tile_x, tile_y} = calculatePointProjection(lat, lng, MAX_ZOOM_LEVEL);
    const isTileExisting = checkTileExisting(tile_x, tile_y, MAX_ZOOM_LEVEL);

    if (isTileExisting) {
      // Even if tile exists, still record user visit event
      AnalyticsService.trackUserAction('tile_revisited', {
        tile_x, tile_y, tile_z: MAX_ZOOM_LEVEL,
        latitude: lat, longitude: lng,
      });
      return;
    }

    // Check exploration speed
    const tileId = `${tile_x}-${tile_y}-${MAX_ZOOM_LEVEL}`;
    const explorationCheck = FraudDetectionService.checkExplorationRate(tileId);
    
    const {tile_center_lng, tile_center_lat} = getTileCenterPoint(
      tile_x,
      tile_y,
      MAX_ZOOM_LEVEL,
    );

    realm.write(() => {
      realm.create('UserLocationModel', {
        lat: tile_center_lat.toFixed(Fix_number),
        lng: tile_center_lng.toFixed(Fix_number),
        _id: new Realm.BSON.ObjectId(),
        zoomLevel: MAX_ZOOM_LEVEL,
        date: Math.floor(Date.now() / 1000),
      });

      realm.create('TileModel', {
        tile_x: tile_x,
        tile_y: tile_y,
        _id: new Realm.BSON.ObjectId(),
        tile_z: MAX_ZOOM_LEVEL,
      });
    });

    recordTileData(tile_center_lat, tile_center_lng);

    // Record location exploration event to analytics service
    AnalyticsService.trackLocationExploration(locationPoint, {
      tileX: tile_x,
      tileY: tile_y,
      tileZ: MAX_ZOOM_LEVEL,
      eventType: '1',
    });

    const tileRequestData = {
      eventType: '1',
      lat: tile_center_lat,
      lng: tile_center_lng,
      tileX: tile_x,
      tileY: tile_y,
      tileZ: MAX_ZOOM_LEVEL,
      // Add anti-cheat information
      fraud_score: fraudCheck.riskScore,
      exploration_suspicious: explorationCheck.isSuspicious,
    };

    sendTileRequest(tileRequestData);
  };

  // The mapping between latitude, longitude and pixels is defined by the web
  // mercator projection.
  const calculatePointProjection = (
    lat: number,
    lng: number,
    zoomLevel: number,
  ) => {
    let siny = Math.sin((lat * Math.PI) / 180);
    // Truncating to 0.9999 effectively limits latitude to 89.189. This is
    // about a third of a tile past the edge of the world tile.
    siny = Math.min(Math.max(siny, -0.9999), 0.9999);
    const p_x = TILE_SIZE * (0.5 + lng / 360);
    const p_y =
      TILE_SIZE * (0.5 - Math.log((1 + siny) / (1 - siny)) / (4 * Math.PI));

    const scale = 1 << zoomLevel;

    const x = Math.floor((p_x * scale) / TILE_SIZE);
    const y = Math.floor((p_y * scale) / TILE_SIZE);

    return {
      tile_x: x,
      tile_y: y,
    };
  };

  const checkTileExisting = (
    tileX: number,
    tileY: number,
    zoomLevel: number,
  ) => {
    const tiles = realm
      .objects('TileModel')
      .filtered(
        'tile_x=$0 AND tile_y=$1 AND tile_z=$2',
        tileX,
        tileY,
        zoomLevel,
      );

    return tiles.length > 0;
  };

  // Check if corresponding value exists in WMSXYCoordinateModel table
  const hasSameZXY = (zxy: string) => {
    const array = realm
      .objects('WMSXYCoordinateModel')
      .filtered('level_xy=$0', zxy);
    if (array.length > 0) {
      return true;
    }
    return false;
  };

  // Record tile x y to database, used for queries when Map renders fog
  const recordTileData = (lat: number, lng: number) => {
    realm.write(() => {
      for (
        let n_level = MIN_ZOOM_LEVEL;
        n_level <= MAX_ZOOM_LEVEL + 4;
        n_level++
      ) {
        const {tile_x, tile_y} = calculatePointProjection(lat, lng, n_level);
        const x_str: string = String(tile_x);
        const y_str: string = String(tile_y);
        const z_str: string = String(n_level);
        const result_zxy = z_str + '-' + x_str + '-' + y_str;
        if (!hasSameZXY(result_zxy)) {
          realm.create('WMSXYCoordinateModel', {
            level_xy: result_zxy,
            _id: new Realm.BSON.ObjectId(),
            date: Math.floor(Date.now() / 1000),
          });
        }
      }
    });
  };

  // get this from Chat GPT, validated the result are correct
  const getTileCenterPoint = (tileX: number, tileY: number, zoom: number) => {
    const n = Math.pow(2, zoom);
    const lng = (tileX / n) * 360 - 180;
    const latRad = Math.atan(Math.sinh(Math.PI * (1 - (2 * tileY + 1) / n)));
    const lat = (latRad * 180) / Math.PI;
    return {tile_center_lat: lat, tile_center_lng: lng};
  };

  const loadSererTileDataIntoLocalDB = (tileData: {
    centerPoint: {lat: number; lng: number};
    tileX: number;
    tileY: number;
    tileZ: number;
  }) => {
    realm.write(() => {
      realm.create('UserLocationModel', {
        lat: tileData.centerPoint.lat.toFixed(Fix_number),
        lng: tileData.centerPoint.lng.toFixed(Fix_number),
        _id: new Realm.BSON.ObjectId(),
        zoomLevel: MAX_ZOOM_LEVEL,
        date: Math.floor(Date.now() / 1000),
      });

      realm.create('TileModel', {
        tile_x: tileData.tileX,
        tile_y: tileData.tileY,
        _id: new Realm.BSON.ObjectId(),
        tile_z: tileData.tileZ,
      });
    });

    recordTileData(tileData.centerPoint.lat, tileData.centerPoint.lng);
  };
  return {
    getLatLngArr,
    deleteAllLocations,
    getLocationCount,
    recordLocationData,
    loadSererTileDataIntoLocalDB,
  };
};
