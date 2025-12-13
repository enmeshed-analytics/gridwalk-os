import { create } from 'zustand'
import { persist } from 'zustand/middleware'

interface SelectedLayer {
  id: string
  name: string
}

interface LayerStore {
  selectedLayers: Map<string, SelectedLayer>
  toggleLayer: (layer: SelectedLayer) => void
  selectLayer: (layer: SelectedLayer) => void
  deselectLayer: (layerId: string) => void
  selectMultiple: (layers: SelectedLayer[]) => void
  clearSelection: () => void
  isSelected: (layerId: string) => boolean
  getSelectedCount: () => number
  getSelectedIds: () => string[]
  getSelectedLayerName: (layerId: string) => string | undefined
  getSelectedLayers: () => SelectedLayer[]
}

export const useLayerStore = create<LayerStore>()(
  persist(
    (set, get) => ({
  selectedLayers: new Map<string, SelectedLayer>(),

  toggleLayer: (layer: SelectedLayer) =>
    set((state) => {
      const newSelected = new Map(state.selectedLayers)
      if (newSelected.has(layer.id)) {
        newSelected.delete(layer.id)
      } else {
        newSelected.set(layer.id, layer)
      }
      return { selectedLayers: newSelected }
    }),

  selectLayer: (layer: SelectedLayer) =>
    set((state) => {
      const newSelected = new Map(state.selectedLayers)
      newSelected.set(layer.id, layer)
      return { selectedLayers: newSelected }
    }),

  deselectLayer: (layerId: string) =>
    set((state) => {
      const newSelected = new Map(state.selectedLayers)
      newSelected.delete(layerId)
      return { selectedLayers: newSelected }
    }),

  selectMultiple: (layers: SelectedLayer[]) =>
    set((state) => {
      const newSelected = new Map(state.selectedLayers)
      layers.forEach(layer => newSelected.set(layer.id, layer))
      return { selectedLayers: newSelected }
    }),

  clearSelection: () =>
    set(() => ({ selectedLayers: new Map<string, SelectedLayer>() })),

  isSelected: (layerId: string) => get().selectedLayers.has(layerId),

  getSelectedCount: () => get().selectedLayers.size,

  getSelectedIds: () => Array.from(get().selectedLayers.keys()),

  getSelectedLayerName: (layerId: string) => get().selectedLayers.get(layerId)?.name,

  getSelectedLayers: () => Array.from(get().selectedLayers.values()),
    }),
    {
      name: 'layer-selection',
      storage: {
        getItem: (name) => {
          const str = localStorage.getItem(name)
          if (!str) return null
          const parsed = JSON.parse(str)
          return {
            ...parsed,
            state: {
              ...parsed.state,
              selectedLayers: new Map(
                (parsed.state.selectedLayers || []).map((layer: SelectedLayer) => [layer.id, layer])
              )
            }
          }
        },
        setItem: (name, value) => {
          const serialized = {
            ...value,
            state: {
              ...value.state,
              selectedLayers: Array.from(value.state.selectedLayers.values())
            }
          }
          localStorage.setItem(name, JSON.stringify(serialized))
        },
        removeItem: (name) => localStorage.removeItem(name),
      },
    }
  )
)
