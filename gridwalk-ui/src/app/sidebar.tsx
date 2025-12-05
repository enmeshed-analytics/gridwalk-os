'use client';

import { useRef, useState } from 'react';
import * as tus from 'tus-js-client';
import useSWR from 'swr';
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "@/components/ui/sheet"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Checkbox } from "@/components/ui/checkbox"
import { Menu, Upload } from "lucide-react"
import { useLayerStore } from "@/stores"

interface Layer {
  id: string;
  status: string;
  name: string;
  upload_type: string;
  total_size: number;
  current_offset: number;
  created_at: string;
  updated_at: string;
}

const fetcher = (url: string) => fetch(url).then(res => {
  if (!res.ok) throw new Error('Failed to fetch');
  return res.json();
});

export function Sidebar() {
  const { data: layers, error, isLoading } = useSWR<Layer[]>(`${process.env.NEXT_PUBLIC_API_BASE_URL}/layers`, fetcher);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [layerName, setLayerName] = useState('');
  const { selectedLayers, toggleLayer, isSelected } = useLayerStore();

  const handleUploadClick = () => {
    fileInputRef.current?.click();
  };

  const handleFileSelect = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (file) {
      console.log('File selected:', {
        name: file.name,
        size: file.size,
        type: file.type,
        lastModified: new Date(file.lastModified).toISOString(),
      });

      setSelectedFile(file);
      setLayerName(file.name.replace(/\.[^/.]+$/, '')); // Remove file extension
      setDialogOpen(true);
    }
  };

  const handleDialogClose = () => {
    setDialogOpen(false);
    setSelectedFile(null);
    setLayerName('');
    // Reset file input
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  };

  const handleUploadConfirm = () => {
    if (!selectedFile || !layerName.trim()) return;

    // Create TUS upload
    const upload = new tus.Upload(selectedFile, {
      endpoint: `${process.env.NEXT_PUBLIC_API_BASE_URL}/layers`,
      metadata: {
        name: layerName.trim(),
        upload_type: 'geojson',
      },
      onError: (error) => {
        console.error('Upload failed:', error);
      },
      onSuccess: () => {
        console.log('Upload completed successfully');
      },
      onProgress: (bytesUploaded, bytesTotal) => {
        const percentage = Math.round((bytesUploaded / bytesTotal) * 100);
        console.log(`Upload progress: ${percentage}%`);
      },
    });

    // Start upload and close dialog
    upload.start();
    handleDialogClose();
  };
  return (
    <>
    <Sheet>
      <SheetTrigger asChild>
        <Button variant="outline" size="icon" className="fixed top-4 right-4 z-50">
          <Menu className="h-4 w-4" />
        </Button>
      </SheetTrigger>
        <SheetContent side="right" className="flex flex-col">
        <SheetHeader>
          <SheetTitle>Map Layers</SheetTitle>
          <SheetDescription>
            Toggle map layers on and off
          </SheetDescription>
        </SheetHeader>
        <div className="mt-6 flex-1 overflow-y-auto px-4">
          {isLoading && (
            <div className="text-center py-4">Loading layers...</div>
          )}
          
          {error && (
            <div className="text-center py-4 text-red-500">
              Failed to load layers
            </div>
          )}
          
          {layers && layers.length > 0 && (
          <div className="space-y-6">
              {/* Available Layers */}
              {layers.filter(layer => layer.status === 'available').length > 0 && (
                <div>
                  <h3 className="text-sm font-medium text-gray-700 mb-2">Layers</h3>
                  <ul className="space-y-2">
                    {layers.filter(layer => layer.status === 'available').map((layer) => (
                      <li 
                        key={layer.id} 
                        className={`p-3 rounded-md border cursor-pointer transition-colors ${
                          isSelected(layer.id) 
                            ? 'bg-green-100 border-green-300 hover:bg-green-200' 
                            : 'bg-green-50 border-green-200 hover:bg-gray-50'
                        }`}
                        onClick={() => toggleLayer(layer.id)}
                      >
                        <div className="flex items-center justify-between">
                          <div className="flex items-center space-x-3">
                            <Checkbox 
                              checked={isSelected(layer.id)}
                              onChange={() => toggleLayer(layer.id)}
                              className="border-2 border-gray-400 data-[state=checked]:bg-green-600 data-[state=checked]:border-green-600 bg-white"
                            />
                            <div className="font-medium">{layer.name}</div>
                          </div>
                          <div className="w-2 h-2 bg-green-500 rounded-full"></div>
                        </div>
                      </li>
                    ))}
                  </ul>
                </div>
              )}

              {/* Uploading Layers */}
              {layers.filter(layer => layer.status === 'uploading').length > 0 && (
                <div>
                  <h3 className="text-sm font-medium text-gray-700 mb-2">Uploading</h3>
                  <ul className="space-y-2">
                    {layers.filter(layer => layer.status === 'uploading').map((layer) => (
                      <li 
                        key={layer.id} 
                        className="p-3 rounded-md border bg-yellow-50 border-yellow-200"
                      >
                        <div className="flex items-center justify-between">
                          <div className="font-medium">{layer.name}</div>
                          <div className="w-2 h-2 bg-yellow-500 rounded-full animate-pulse"></div>
                        </div>
                        <div className="mt-2">
                          <div className="w-full bg-gray-200 rounded-full h-2">
                            <div 
                              className="bg-yellow-500 h-2 rounded-full transition-all duration-300" 
                              style={{ width: `${Math.round((layer.current_offset / layer.total_size) * 100)}%` }}
                            ></div>
                          </div>
                          <div className="text-xs text-gray-500 mt-1">
                            {Math.round((layer.current_offset / layer.total_size) * 100)}% uploaded
                          </div>
                        </div>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
          </div>
          )}
          
          {layers && layers.length === 0 && (
            <div className="text-center py-4 text-gray-500">
              No layers found
            </div>
          )}
        </div>
          <div className="mt-auto pt-4 pb-4 px-4 border-t flex justify-center">
            <Button variant="default" onClick={handleUploadClick}>
              <Upload className="h-4 w-4 mr-2" />
              Upload Layer
            </Button>
            <input
              ref={fileInputRef}
              type="file"
              className="hidden"
              onChange={handleFileSelect}
              accept=".geojson,.json"
            />
          </div>
      </SheetContent>
    </Sheet>

      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Upload Layer</DialogTitle>
            <DialogDescription>
              Enter a name for your layer and confirm the upload.
            </DialogDescription>
          </DialogHeader>
          
          {selectedFile && (
            <div className="space-y-4">
              <div className="grid grid-cols-2 gap-2 text-sm">
                <div><strong>File:</strong> {selectedFile.name}</div>
                <div><strong>Size:</strong> {(selectedFile.size / 1024).toFixed(1)} KB</div>
                <div><strong>Type:</strong> {selectedFile.type}</div>
              </div>
              
              <div className="space-y-2">
                <Label htmlFor="layer-name">Layer Name</Label>
                <Input
                  id="layer-name"
                  value={layerName}
                  onChange={(e) => setLayerName(e.target.value)}
                  placeholder="Enter layer name"
                />
              </div>
              
              <div className="flex justify-end space-x-2">
                <Button variant="outline" onClick={handleDialogClose}>
                  Cancel
                </Button>
                <Button 
                  onClick={handleUploadConfirm}
                  disabled={!layerName.trim()}
                >
                  Upload
                </Button>
              </div>
            </div>
          )}
        </DialogContent>
      </Dialog>
    </>
  )
}
