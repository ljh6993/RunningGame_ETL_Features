import BackgroundGeolocation from 'react-native-background-geolocation';
export const MIN_ZOOM_LEVEL = 2;
export const MAX_ZOOM_LEVEL = 14;
export const DEFAULT_TILE_SIZE = 96;

// expiration time for fetch gloabl ranking
export const GLOBAL_RANK_EXPIRATION_DURATION = 3600 * 1000; //3600 * 1000; // 1 hour in milliseconds
export const GLOBAL_RANK_LIST_LENGTH = 100;

export const WalletConnectProjectId = '5d7fd6cfc1866d063487db3c810be701';
export const WalletProviderMetadata = {
  name: 'Truworld App',
  description: 'Truworld App',
  url: 'https://trumen.world/',
  icons: ['https://trumen.world/favicon.png'],
  redirect: {
    native: 'trumenapp://',
    universal: 'trumen.world',
  },
};
// On-chain balance is a large hexadecimal integer, need to divide by this number to get real balance
export const Decimial = 10 ** 18;

export const AsyncStorageKeys = {
  ACCESS_TOKEN: 'ACCESS_TOKEN',
  IS_NEW_USER: 'IS_NEW_USER',
  NEED_SHOW_WALLET_CONNECT: 'NEED_SHOW_WALLET_CONNECT',
  GLOBAL_TOP_USERS: 'GLOBAL_TOP_USERS',
  GENERAL_LOCATION_ACCESS: 'GENERAL_LOCATION_ACCESS',
  DEVICE_TOKEN: 'DEVICE_TOKEN',
  LAST_LOGIN_TIME: 'LAST_LOGIN_TIME',
  ONE_DAY_SCREENSHOT_CLAIM: 'ONE_DAY_SCREENSHOT_CLAIM',
  TRUMEN_ID: 'TRUMEN_ID',
  USER_INFO: 'USER_INFO',
  DEVICE_ID: 'DEVICE_ID',
  DEVICE_INFO: 'DEVICE_INFO',
  AB_TEST_ASSIGNMENTS: 'AB_TEST_ASSIGNMENTS',
};

export const BackgroundGeolocationConfig = {
  // Geolocation Config
  desiredAccuracy: BackgroundGeolocation.DESIRED_ACCURACY_LOWEST,
  distanceFilter: 20, // Unit is meters
  debug: false, // <-- enable this hear sounds for background-geolocation life-cycle.
  // Activity Recognition
  // stopTimeout: 5,
  // // Application config
  stopOnTerminate: false, // <-- Allow the background-service to continue tracking when user closes the app.
  startOnBoot: true, // <-- Auto start tracking when device is powered-up.
};
export default {
  MIN_ZOOM_LEVEL,
  MAX_ZOOM_LEVEL,
  DEFAULT_TILE_SIZE,
  AsyncStorageKeys,
  BackgroundGeolocationConfig,
};
