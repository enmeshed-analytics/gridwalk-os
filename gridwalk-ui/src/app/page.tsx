'use client';

import { useEffect, useRef, useState } from 'react';
import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import { Sidebar } from './sidebar';
import { useLayerStore } from '@/stores';

interface TokenResponse {
  access_token: string;
  expires_in: string;
  issued_at: string;
  token_type: string;
}

export default function Home() {
  const mapContainer = useRef<HTMLDivElement>(null);
  const map = useRef<maplibregl.Map | null>(null);
  const selectedLayers = useLayerStore(state => state.selectedLayers);
  const [mapLoaded, setMapLoaded] = useState(false);

  // Token management refs
  const tokenRef = useRef<string | null>(null);
  const tokenExpiryRef = useRef<number>(0);
  const refreshPromiseRef = useRef<Promise<string> | null>(null);
  const tokenRefreshTimerRef = useRef<number | null>(null);

  // Function to schedule token refresh
  const scheduleTokenRefresh = (expiresInMs: number) => {
    // Clear existing timer
    if (tokenRefreshTimerRef.current) {
      console.log('Clearing previous token refresh timer:', tokenRefreshTimerRef.current);
      clearTimeout(tokenRefreshTimerRef.current);
    }

    // Schedule refresh at 90% of token lifetime (10% buffer before expiry)
    const refreshDelay = Math.max(expiresInMs * 0.9, 30000); // minimum 30 seconds
    console.log(`Scheduling token refresh in ${Math.round(refreshDelay / 1000)}s (90% of ${Math.round(expiresInMs / 1000)}s lifetime)`);

    tokenRefreshTimerRef.current = setTimeout(async () => {
      console.log('Background token refresh timer fired');
      try {
        await fetchToken();
        console.log('Background token refresh completed successfully');
      } catch (error) {
        console.error('Background token refresh failed:', error);
        // Retry in 30 seconds on failure
        console.log('Scheduling retry in 30 seconds');
        setTimeout(() => fetchToken().catch(console.error), 30000);
      }
    }, refreshDelay);
    console.log('Token refresh timer set with ID:', tokenRefreshTimerRef.current);
  };

  // Function to fetch a new token
  const fetchToken = async (): Promise<string> => {
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/service/os/token`, {
      method: 'GET',
    });

      if (!response.ok) {
        throw new Error(`Failed to fetch token: ${response.statusText}`);
      }

      const data: TokenResponse = await response.json();

      // Store token and expiry time
      tokenRef.current = data.access_token;
      tokenExpiryRef.current = Date.now() + (parseInt(data.expires_in) * 1000);

      // Schedule next token refresh
      scheduleTokenRefresh(parseInt(data.expires_in) * 1000);

      return data.access_token;
    };

  // Function to get a valid token, refreshing if necessary
  const getToken = async (): Promise<string> => {
    if (tokenRef.current && Date.now() < tokenExpiryRef.current - 10) {
      return tokenRef.current;
    }
    if (!refreshPromiseRef.current) {
      refreshPromiseRef.current = fetchToken().finally(() => {
        refreshPromiseRef.current = null;
      });
    }
    return refreshPromiseRef.current;
  };


  useEffect(() => {
    if (map.current) return; // initialize map only once
    
    const initializeMap = async () => {
      if (mapContainer.current) {
        const token = await getToken();
        map.current = new maplibregl.Map({
          container: mapContainer.current,
          style: 'https://api.os.uk/maps/vector/v1/vts/resources/styles?srs=3857',
          center: [-0.0754, 51.5055], // London coordinates [lng, lat] - Tower Bridge
          zoom: 10,
          maxBounds: [
            [ -10.76418, 49.528423 ],
            [ 1.9134116, 61.331151 ]
          ],
          navigationControl: true,
          projection: 'mercator', // Explicitly use EPSG:3857 Web Mercator
          transformRequest: (url, resourceType) => {
            if (url.includes('api.os.uk')) {
              return {
                url: url,
                headers: {
                  'Authorization': `Bearer ${tokenRef.current}`
                }
              };
            }
            return { url };
          }
        });

          // Set up map load event handler
          map.current.on('load', () => {
            setMapLoaded(true);
          });
      }
    };

    initializeMap();

    return () => {
      if (map.current) {
        map.current.remove();
      }
        // Clear token refresh timer
        if (tokenRefreshTimerRef.current) {
          clearTimeout(tokenRefreshTimerRef.current);
        }
    };
  }, []);

   // Dynamic layer management based on selected layers
   useEffect(() => {
     if (!map.current || !mapLoaded) return;

     const currentMapLayers = new Set<string>();
     
     // Get currently active layer sources on the map
     const mapStyle = map.current.getStyle();
     if (mapStyle.sources) {
       Object.keys(mapStyle.sources).forEach(sourceId => {
         if (sourceId.match(/^[a-f0-9-]{36}$/)) { // UUID pattern for our layers
           currentMapLayers.add(sourceId);
         }
       });
     }

     // Add new layers that are selected but not on map
     selectedLayers.forEach(layerId => {
       if (!currentMapLayers.has(layerId)) {
         addLayerToMap(layerId);
       }
     });

     // Remove layers that are no longer selected
     currentMapLayers.forEach(layerId => {
       if (!selectedLayers.has(layerId)) {
         removeLayerFromMap(layerId);
       }
     });
   }, [selectedLayers, mapLoaded]);

   const addLayerToMap = (layerId: string) => {
     if (!map.current) return;

     try {
       // Add the vector tile source
       map.current.addSource(layerId, {
         type: 'vector',
         tiles: [`${process.env.NEXT_PUBLIC_API_BASE_URL}/layers/${layerId}/tiles/{z}/{x}/{y}`]
       });

       // Add fill layer
       map.current.addLayer({
         id: `${layerId}-fill`,
         type: 'fill',
         source: layerId,
         'source-layer': layerId,
         paint: {
           'fill-color': '#0080ff',
           'fill-opacity': 0.5
         }
       });

       // Add line layer
       map.current.addLayer({
         id: `${layerId}-line`,
         type: 'line',
         source: layerId,
         'source-layer': layerId,
         paint: {
           'line-color': '#0080ff',
           'line-width': 2,
           'line-opacity': 0.8
         }
       });

       console.log(`Added layer: ${layerId}`);
     } catch (error) {
       console.error(`Failed to add layer ${layerId}:`, error);
     }
   };

   const removeLayerFromMap = (layerId: string) => {
     if (!map.current) return;

     try {
       // Remove layers
       if (map.current.getLayer(`${layerId}-fill`)) {
         map.current.removeLayer(`${layerId}-fill`);
       }
       if (map.current.getLayer(`${layerId}-line`)) {
         map.current.removeLayer(`${layerId}-line`);
       }
       
       // Remove source
       if (map.current.getSource(layerId)) {
         map.current.removeSource(layerId);
       }

       console.log(`Removed layer: ${layerId}`);
     } catch (error) {
       console.error(`Failed to remove layer ${layerId}:`, error);
     }
   };

  return (
    <>
      <Sidebar />
    <div 
      ref={mapContainer} 
      className="w-full h-screen"
      style={{ width: '100vw', height: '100vh' }}
    />
    </>
  );
}
