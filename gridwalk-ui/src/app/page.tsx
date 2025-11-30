'use client';

import { useEffect, useRef, useState } from 'react';
import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import { Sidebar } from './sidebar';
import { useLayerStore } from '@/stores';

export default function Home() {
  const mapContainer = useRef<HTMLDivElement>(null);
  const map = useRef<maplibregl.Map | null>(null);
  const selectedLayers = useLayerStore(state => state.selectedLayers);
  const [mapLoaded, setMapLoaded] = useState(false);
  
  const OS_API_KEY = process.env.NEXT_PUBLIC_OS_API_KEY;

  useEffect(() => {
    if (map.current) return; // initialize map only once
    
    if (mapContainer.current) {
      map.current = new maplibregl.Map({
        container: mapContainer.current,
        style: `https://api.os.uk/maps/vector/v1/vts/resources/styles?srs=3857&key=${OS_API_KEY}`,
        center: [-0.0754, 51.5055], // London coordinates [lng, lat] - Tower Bridge
        zoom: 10,
        maxBounds: [
          [ -10.76418, 49.528423 ],
          [ 1.9134116, 61.331151 ]
        ],
        navigationControl: true,
        projection: 'mercator' // Explicitly use EPSG:3857 Web Mercator
      });

        // Set up map load event handler
        map.current.on('load', () => {
          setMapLoaded(true);
        });
    }

    return () => {
      if (map.current) {
        map.current.remove();
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
