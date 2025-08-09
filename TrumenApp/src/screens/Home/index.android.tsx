import React, {useState, useEffect, useRef, useCallback} from 'react';
import {Dimensions, View, Alert} from 'react-native';
import MapView, {WMSTile, Marker, Region} from 'react-native-maps';
import i18n from '@Strings';
import {useNavigation, useRoute} from '@react-navigation/native';
import {HomeScreenNavigationProp, ProfileScreenRouteProp} from '@Types/route';
import {useLocalDB} from '../../db/useLocalDB';
import {useAppSelector, useAppDispatch} from '../../redux/hooks';
import MainButton from '@Components/Home/MainButton';
import ShareView from '@Components/Home/ShareView';
import {LocationButtonWithOuterView} from '@Components/Home/LocationButton';
import BackgroundGeolocation, {
  Subscription,
} from 'react-native-background-geolocation';
import {
  MIN_ZOOM_LEVEL,
  MAX_ZOOM_LEVEL,
  DEFAULT_TILE_SIZE,
  BackgroundGeolocationConfig,
} from '../../utils/constants';
import {approximateDeltaForZoomLevel} from '../../utils';
import {styles} from './styles';
import RadialGradient from 'react-native-radial-gradient';
import LinearGradient from 'react-native-linear-gradient';
import UserMarker from '../../components/Home/UserMarker/UserMarker';
import TopButtons from '@Components/Home/TopButtons';
import ChallengeButton from '@Components/Home/Challenge';
import BottomButtons from '@Components/Home/BottomButtons';
import LeftButtons from '@Components/Home/ChestStatus';
import {Coordinate} from '@Models/coordinate';
import AirdropItem from '@Components/AirdropItem';
import {getAirdropsAction} from '@Redux/airdrop/airdropSlice';
import {fetchChestAction} from '@Redux/chest/chestSlice';
import {PERMISSIONS, checkMultiple} from 'react-native-permissions';
import IsInBlockedCountries from '../../utils/countryLocationCheck';
import NotSupported from '@Components/NoSupportScreen';
import AsyncStorage from '@react-native-async-storage/async-storage';
import {AsyncStorageKeys} from '../../utils/constants';
import GuideModal from './GuideModal';
import messaging, {
  FirebaseMessagingTypes,
} from '@react-native-firebase/messaging';
import notifee from '@notifee/react-native';
import ChestUnlcokMarker from '@Components/Home/ChestUnlcokMarker';
import ChestMarker from '@Components/Home/ChestMarker';
import ShowClaimedReward from '@Components/Home/ShowClaimedReward';
import debuounce from 'lodash/debounce';
import {captureRef} from 'react-native-view-shot';
import Share from 'react-native-share';

const Home = () => {
  const {data: airdrops} = useAppSelector(state => state.airdrops);

  const {chests} = useAppSelector(state => state.chests);
  const {isAppActive, isLocationAlwaysAccess} = useAppSelector(
    state => state.appState,
  );
  const {isEnableUplaodData} = useAppSelector(state => state.appState);

  const mapTypes = ['standard', 'hybrid'];
  const [currentMapType, setCurrentMapType] = useState(mapTypes[0]);
  const [isBlocked, setIsBlocked] = useState(false);
  // Function to cycle through map types
  const changeMapType = () => {
    setCurrentMapType(prevType => {
      const currentIndex = mapTypes.indexOf(prevType);
      const nextIndex = (currentIndex + 1) % mapTypes.length;
      return mapTypes[nextIndex];
    });
  };

  // Calculate tileSize dynamically
  // State for dynamic tile size
  const [tileSize, setTileSize] = useState(DEFAULT_TILE_SIZE);
  const [zoomLevel, setZoomLevel] = useState(MIN_ZOOM_LEVEL);

  // for what are fogs modal, when user is new user, show the modal, else, neven show it
  const [isGuideModalVisible, setGuideModalVisible] = useState(false);
  useEffect(() => {
    AsyncStorage.getItem(AsyncStorageKeys.IS_NEW_USER).then(value => {
      if (value === 'true') {
        setGuideModalVisible(true);
        // when modal is close, set the value to false
        AsyncStorage.setItem(AsyncStorageKeys.IS_NEW_USER, 'false');
      }
    });
  }, []);

  useEffect(() => {
    const screenWidth = Dimensions.get('window').width;
    const standardScreenWidth = 1080; // Define a standard screen width
    const scale = screenWidth / standardScreenWidth;
    const adjustedTileSize = DEFAULT_TILE_SIZE * scale;

    setTileSize(adjustedTileSize);
  }, []);

  const dispatch = useAppDispatch();
  const {data: profile} = useAppSelector(state => state.profile);

  const userAvatar = profile?.avatar_url;

  const navigation = useNavigation<HomeScreenNavigationProp>();
  const route = useRoute<ProfileScreenRouteProp>();

  const localDB = useLocalDB();

  const [userCoordinate, setUserCoordinate] = React.useState<Coordinate | null>(
    null,
  );
  const [region, setRegion] = React.useState(
    userCoordinate || {
      latitudeDelta: 0,
      longitudeDelta: 0.0005 + Math.random() / 1000,
      latitude: 37.33058271,
      longitude: -122.02924708,
    },
  );

  // control the main button display
  const [isMainButtonOn, setMainButtonIsOn] = useState(false);
  const toggleMainButtonIsOn = () => {
    setMainButtonIsOn(!isMainButtonOn); // Update isOn state
  };

  // control Location privacy Button display
  const [isLocationButtonOn, setLocationButtonIsOn] = useState(false);
  const toggleLocationButtonIsOn = () => {
    setLocationButtonIsOn(!isLocationButtonOn);
  };

  // control share display screen
  const [isShareScreenOn, setShareScreenIsOn] = useState(false);

  const toggleShareScreenIsOn = useCallback(() => {
    setShareScreenIsOn(isShareScreenOn => !isShareScreenOn);
  }, [isShareScreenOn]);

  const mapView = useRef(null);
  const shareViewRef = useRef<View>(null);

  const tilePath =
    'https://demo.geo-solutions.it/geoserver/tiger/wms?service=WMS&version=1.1.0&request=GetMap&layers=tiger:poi&styles=&bbox={minX},{minY},{maxX},{maxY}&width={width}&height={height}&srs=EPSG:900913&format=image/png&transparent=true&format_options=dpi:213';

  const handleRegionChange = (newRegion: Region) => {
    console.log('region change');
    // Check if the new region is different from the current one to avoid unnecessary updates
    if (
      region.latitude !== newRegion.latitude ||
      region.longitude !== newRegion.longitude ||
      region.latitudeDelta !== newRegion.latitudeDelta ||
      region.longitudeDelta !== newRegion.longitudeDelta
    ) {
      setRegion(newRegion);
      setZoomLevel(calculateZoomLevel(newRegion.latitudeDelta));
    }
  };

  const calculateZoomLevel = (latitudeDelta: number) => {
    const screenHeightInPixels = Dimensions.get('window').height; // Change this based on your screen height
    const latitudeDeltaToPixel = latitudeDelta * screenHeightInPixels;
    const zoomLevel = Math.log2(
      (360 * (screenHeightInPixels / 2)) / latitudeDeltaToPixel,
    );
    console.log(zoomLevel);
    return Math.floor(zoomLevel);
  };

  // Debouncing ensures that your state updates only occur after a certain amount of time
  // has elapsed since the last event, preventing rapid state changes that could
  // lead to infinite loops.

  const debouncedHandleRegionChange = debuounce(handleRegionChange, 200);

  const centerToUser = async () => {
    const latitudeDelta = approximateDeltaForZoomLevel(MAX_ZOOM_LEVEL);
    const longitudeDelta = latitudeDelta; // Assuming a square map view

    mapView.current?.animateToRegion(
      {
        latitude: userCoordinate?.latitude,
        longitude: userCoordinate?.longitude,
        latitudeDelta: latitudeDelta,
        longitudeDelta: longitudeDelta,
      },
      666,
    );
  };

  const onStartLocation = async (isEnableUplaodData = true) => {
    BackgroundGeolocation.removeAllListeners();
    BackgroundGeolocation.stop();
    const onLocation: Subscription = BackgroundGeolocation.onLocation(
      location => {
        // Alert.alert('location', location.coords.latitude + '');
        if (location.coords) {
          const isBlocked = IsInBlockedCountries({
            latitude: location.coords.latitude,
            longitude: location.coords.longitude,
          });

          if (isBlocked) {
            BackgroundGeolocation.stop();
            BackgroundGeolocation.removeAllListeners();
            setIsBlocked(true);
            return;
          }
          if (isEnableUplaodData) {
            localDB.recordLocationData(
              location.coords.latitude,
              location.coords.longitude,
              location.coords.accuracy,
              location.coords.speed
            );
          }
          setUserCoordinate({
            latitude: location.coords.latitude,
            longitude: location.coords.longitude,
          });
        }
      },
    );

    /// 2. ready the plugin.
    BackgroundGeolocation.ready(BackgroundGeolocationConfig).then(state => {
      BackgroundGeolocation.start();
      console.log('- BackgroundGeolocation is configured and ready: ');
    });
  };

  useEffect(() => {
    if (isAppActive) {
      checkMultiple([
        PERMISSIONS.ANDROID.ACCESS_FINE_LOCATION,
        PERMISSIONS.ANDROID.ACCESS_BACKGROUND_LOCATION,
      ]).then(statuses => {
        if (
          statuses[PERMISSIONS.ANDROID.ACCESS_FINE_LOCATION] ||
          statuses[PERMISSIONS.ANDROID.ACCESS_BACKGROUND_LOCATION]
        ) {
          onStartLocation(isEnableUplaodData);
        } else {
          console.log('no Permissions gratned');
        }
      });
    } else {
      if (!isLocationAlwaysAccess) {
        BackgroundGeolocation.stop();
        BackgroundGeolocation.removeAllListeners();
      }
    }
  }, []);

  useEffect(() => {
    dispatch(getAirdropsAction());
  }, []);

  useEffect(() => {
    // todo If Treasure data has been retrieved, don't retrieve again, default chest is null
    if (userCoordinate && !chests) {
      dispatch(
        fetchChestAction({
          currentUserLocation: {
            latitude: userCoordinate.latitude,
            longitude: userCoordinate.longitude,
          },
        }),
      );
    }
  }, []);

  // Display notification when app is in foreground
  const onMessageReceived = (message: FirebaseMessagingTypes.RemoteMessage) => {
    if (message.notification) {
      const {body, title} = message.notification;
      notifee.displayNotification({
        title: title,
        body: body,
      });
    }
  };
  useEffect(() => {
    messaging().onMessage(onMessageReceived);
    notifee.onBackgroundEvent(async ({type, detail}) => {
      // type =1 indicates user pressed notification
      if (type === 1) {
        notifee.setBadgeCount(0);
        navigation.navigate('Friends');
      }
    });
    notifee.onForegroundEvent(({type, detail}) => {
      // type =1 indicates user pressed notification
      if (type === 1) {
        notifee.setBadgeCount(0);
        navigation.navigate('Friends');
      }
    });
  }, []);

  return (
    <>
      {!isBlocked && (
        <View style={styles.mainView} ref={shareViewRef}>
          {/* <GuideModal
            showModal={isGuideModalVisible}
            onClose={() => setGuideModalVisible(false)}
          /> */}
          <MapView
            zoomControlEnabled={true}
            zoomEnabled={true}
            style={{width: '100%', height: '125%', top: -50}}
            ref={mapView}
            mapType={currentMapType} //use hybridFlyover for 3D but it will crash and the tiles are mismatched.
            userInterfaceStyle={'dark'}
            onRegionChange={debouncedHandleRegionChange}
            maxZoomLevel={MAX_ZOOM_LEVEL}
            //  minZoomLevel={MIN_ZOOM_LEVEL}
            minDelta={approximateDeltaForZoomLevel(MAX_ZOOM_LEVEL)}
            maxDelta={50}>
            <WMSTile
              urlTemplate={tilePath}
              zIndex={-1}
              opacity={1}
              // TODO: Tile size needs to change according to device.
              // the tile is super small on smaller screens
              tileSize={tileSize}
              tileCacheMaxAge={0}
            />

            {userCoordinate && (
              <Marker
                zIndex={99}
                coordinate={userCoordinate}
                centerOffset={{x: 0, y: -30}}>
                <View style={styles.MarkerContainer}>
                  <UserMarker avatarUrl={userAvatar} />
                </View>
              </Marker>
            )}
            {/* <ShowClaimedReward /> */}
            {/* {chests?.map(chest => {
              return (
                <ChestUnlcokMarker
                  userCoordinate={userCoordinate}
                  chest={chest}
                  zoomLevel={zoomLevel}
                />
              );
            })} */}

            {/* {chests?.map(chest => {
              return (
                <>
                  <ChestMarker
                    userCoordinate={userCoordinate}
                    chest={chest}
                    zoomLevel={zoomLevel}
                  />
                </>
              );
            })} */}
            {/* {airdrops.map((airdrop, index) => (
              <AirdropItem
                onPress={() => {
                  navigation.navigate('Airdrop', {airdrop: airdrop});
                }}
                airdrop={airdrop}
                zoomLevel={zoomLevel}
              />
            ))} */}
          </MapView>
          {/* <RadialGradient
            style={styles.radialGradient}
            colors={['transparent', '#000000']}
            stops={[0.3, 1]}
            // center={[150, 150]}
            pointerEvents="none" // Allow touch events to pass through
            radius={700}
          /> */}

          <ShareView
            key={'shareview'}
            isOn={isShareScreenOn}
            toggleIsOn={toggleShareScreenIsOn}
            shareViewRef={shareViewRef}
          />

          {/* if share on, show the screen View, make all button disappear */}
          {!isShareScreenOn && (
            <>
              <MainButton
                profile={profile}
                isOn={isMainButtonOn}
                toggleIsOn={toggleMainButtonIsOn}
                // toggleModal={toggleModal}
                navigation={navigation}
              />

              {isLocationButtonOn && (
                <LocationButtonWithOuterView
                  toggleIsOn={toggleLocationButtonIsOn}
                />
              )}

              <TopButtons
                toggleShareScreenIsOn={toggleShareScreenIsOn}
                onStartLocation={onStartLocation}
              />
              <ChallengeButton />
              <BottomButtons
                navigation={navigation}
                isMainButtonOn={isMainButtonOn}
                setMainButtonIsOn={toggleMainButtonIsOn}
                centerToUser={centerToUser}
              />
            </>
          )}
        </View>
      )}
      {isBlocked && <NotSupported />}
    </>
  );
};

export default Home;
