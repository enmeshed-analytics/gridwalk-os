import { create } from 'zustand'
import { persist } from 'zustand/middleware'

interface LayerStore {
  selectedLayers: Set<string>
  toggleLayer: (layerId: string) => void
  selectLayer: (layerId: string) => void
  deselectLayer: (layerId: string) => void
  selectMultiple: (layerIds: string[]) => void
  clearSelection: () => void
  isSelected: (layerId: string) => boolean
  getSelectedCount: () => number
  getSelectedIds: () => string[]
}

export const useLayerStore = create<LayerStore>()(
  persist(
    (set, get) => ({
  selectedLayers: new Set<string>(),

  toggleLayer: (layerId: string) =>
    set((state) => {
      const newSelected = new Set(state.selectedLayers)
      if (newSelected.has(layerId)) {
        newSelected.delete(layerId)
      } else {
        newSelected.add(layerId)
      }
      return { selectedLayers: newSelected }
    }),

  selectLayer: (layerId: string) =>
    set((state) => {
      const newSelected = new Set(state.selectedLayers)
      newSelected.add(layerId)
      return { selectedLayers: newSelected }
    }),

  deselectLayer: (layerId: string) =>
    set((state) => {
      const newSelected = new Set(state.selectedLayers)
      newSelected.delete(layerId)
      return { selectedLayers: newSelected }
    }),

  selectMultiple: (layerIds: string[]) =>
    set((state) => {
      const newSelected = new Set(state.selectedLayers)
      layerIds.forEach(id => newSelected.add(id))
      return { selectedLayers: newSelected }
    }),

  clearSelection: () =>
    set(() => ({ selectedLayers: new Set<string>() })),

  isSelected: (layerId: string) => get().selectedLayers.has(layerId),

  getSelectedCount: () => get().selectedLayers.size,

  getSelectedIds: () => Array.from(get().selectedLayers),
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
              selectedLayers: new Set(parsed.state.selectedLayers || [])
            }
          }
        },
        setItem: (name, value) => {
          const serialized = {
            ...value,
            state: {
              ...value.state,
              selectedLayers: Array.from(value.state.selectedLayers)
            }
          }
          localStorage.setItem(name, JSON.stringify(serialized))
        },
        removeItem: (name) => localStorage.removeItem(name),
      },
    }
  )
)
