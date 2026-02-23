local srid = 3857

local tables = {}

tables.roads = osm2pgsql.define_way_table('roads', {
    { column = 'name', type = 'text' },
    { column = 'class', type = 'text' },
    { column = 'surface', type = 'text' },
    { column = 'oneway', type = 'bool' },
    { column = 'ref', type = 'text' },
    { column = 'geom', type = 'linestring', projection = srid, not_null = true },
}, { schema = 'osm' })

tables.railways = osm2pgsql.define_way_table('railways', {
    { column = 'name', type = 'text' },
    { column = 'class', type = 'text' },
    { column = 'geom', type = 'linestring', projection = srid, not_null = true },
}, { schema = 'osm' })

tables.buildings = osm2pgsql.define_area_table('buildings', {
    { column = 'name', type = 'text' },
    { column = 'type', type = 'text' },
    { column = 'geom', type = 'geometry', projection = srid, not_null = true },
}, { schema = 'osm' })

tables.water = osm2pgsql.define_area_table('water', {
    { column = 'name', type = 'text' },
    { column = 'class', type = 'text' },
    { column = 'geom', type = 'geometry', projection = srid, not_null = true },
}, { schema = 'osm' })

tables.waterways = osm2pgsql.define_way_table('waterways', {
    { column = 'name', type = 'text' },
    { column = 'class', type = 'text' },
    { column = 'geom', type = 'linestring', projection = srid, not_null = true },
}, { schema = 'osm' })

tables.landuse = osm2pgsql.define_area_table('landuse', {
    { column = 'name', type = 'text' },
    { column = 'class', type = 'text' },
    { column = 'geom', type = 'geometry', projection = srid, not_null = true },
}, { schema = 'osm' })

tables.natural = osm2pgsql.define_area_table('natural', {
    { column = 'name', type = 'text' },
    { column = 'class', type = 'text' },
    { column = 'geom', type = 'geometry', projection = srid, not_null = true },
}, { schema = 'osm' })

tables.places = osm2pgsql.define_node_table('places', {
    { column = 'name', type = 'text' },
    { column = 'class', type = 'text' },
    { column = 'population', type = 'int' },
    { column = 'geom', type = 'point', projection = srid, not_null = true },
}, { schema = 'osm' })

tables.boundaries = osm2pgsql.define_relation_table('boundaries', {
    { column = 'name', type = 'text' },
    { column = 'admin_level', type = 'int' },
    { column = 'geom', type = 'multilinestring', projection = srid, not_null = true },
}, { schema = 'osm' })

-- Road class mapping from highway tag
local road_classes = {
    motorway = 'motorway', motorway_link = 'motorway',
    trunk = 'trunk', trunk_link = 'trunk',
    primary = 'primary', primary_link = 'primary',
    secondary = 'secondary', secondary_link = 'secondary',
    tertiary = 'tertiary', tertiary_link = 'tertiary',
    residential = 'residential',
    unclassified = 'unclassified',
    living_street = 'residential',
    service = 'service',
    track = 'track',
    path = 'path',
    footway = 'path',
    cycleway = 'path',
    bridleway = 'path',
    steps = 'path',
    pedestrian = 'pedestrian',
}

-- Landuse classes to import
local landuse_classes = {
    residential = true, commercial = true, industrial = true,
    retail = true, farmland = true, farmyard = true,
    cemetery = true, allotments = true, meadow = true,
    orchard = true, vineyard = true, quarry = true,
    military = true,
}

-- Natural classes to import
local natural_classes = {
    wood = true, scrub = true, heath = true,
    grassland = true, wetland = true, beach = true,
    sand = true, bare_rock = true, glacier = true,
}

-- Water area tags
local water_classes = {
    water = true, reservoir = true, basin = true,
    riverbank = true,
}

function osm2pgsql.process_node(object)
    local tags = object.tags
    local place = tags.place

    if place then
        local pop = tonumber(tags.population)
        tables.places:insert({
            name = tags.name,
            class = place,
            population = pop,
            geom = object:as_point(),
        })
    end
end

function osm2pgsql.process_way(object)
    local tags = object.tags

    -- Roads
    local highway = tags.highway
    if highway and road_classes[highway] then
        tables.roads:insert({
            name = tags.name,
            class = road_classes[highway],
            surface = tags.surface,
            oneway = tags.oneway == 'yes',
            ref = tags.ref,
            geom = object:as_linestring(),
        })
        return
    end

    -- Railways
    local railway = tags.railway
    if railway == 'rail' or railway == 'light_rail' or railway == 'subway' or railway == 'tram' then
        tables.railways:insert({
            name = tags.name,
            class = railway,
            geom = object:as_linestring(),
        })
        return
    end

    -- Waterways
    local waterway = tags.waterway
    if waterway == 'river' or waterway == 'stream' or waterway == 'canal' or waterway == 'drain' or waterway == 'ditch' then
        tables.waterways:insert({
            name = tags.name,
            class = waterway,
            geom = object:as_linestring(),
        })
        return
    end

    -- Area features (only if the way is closed)
    if not object.is_closed then
        return
    end

    -- Buildings
    if tags.building then
        tables.buildings:insert({
            name = tags.name,
            type = tags.building,
            geom = object:as_polygon(),
        })
        return
    end

    -- Water
    local natural_tag = tags.natural
    if natural_tag == 'water' or tags.waterway == 'riverbank' or water_classes[tags.water] then
        tables.water:insert({
            name = tags.name,
            class = tags.water or natural_tag or 'water',
            geom = object:as_polygon(),
        })
        return
    end

    -- Landuse
    local landuse = tags.landuse
    if landuse and landuse_classes[landuse] then
        tables.landuse:insert({
            name = tags.name,
            class = landuse,
            geom = object:as_polygon(),
        })
        return
    end

    -- Leisure as landuse
    local leisure = tags.leisure
    if leisure == 'park' or leisure == 'garden' or leisure == 'playground' or leisure == 'pitch' then
        tables.landuse:insert({
            name = tags.name,
            class = leisure,
            geom = object:as_polygon(),
        })
        return
    end

    -- Natural areas
    if natural_tag and natural_classes[natural_tag] then
        tables.natural:insert({
            name = tags.name,
            class = natural_tag,
            geom = object:as_polygon(),
        })
        return
    end
end

function osm2pgsql.process_relation(object)
    local tags = object.tags
    local type = tags.type

    -- Boundary relations
    if type == 'boundary' and tags.boundary == 'administrative' then
        local admin_level = tonumber(tags.admin_level)
        if admin_level then
            tables.boundaries:insert({
                name = tags.name,
                admin_level = admin_level,
                geom = object:as_multilinestring(),
            })
        end
        return
    end

    -- Multipolygon relations
    if type == 'multipolygon' then
        -- Buildings
        if tags.building then
            tables.buildings:insert({
                name = tags.name,
                type = tags.building,
                geom = object:as_multipolygon(),
            })
            return
        end

        -- Water
        local natural_tag = tags.natural
        if natural_tag == 'water' or water_classes[tags.water] then
            tables.water:insert({
                name = tags.name,
                class = tags.water or natural_tag or 'water',
                geom = object:as_multipolygon(),
            })
            return
        end

        -- Landuse
        local landuse = tags.landuse
        if landuse and landuse_classes[landuse] then
            tables.landuse:insert({
                name = tags.name,
                class = landuse,
                geom = object:as_multipolygon(),
            })
            return
        end

        -- Leisure as landuse
        local leisure = tags.leisure
        if leisure == 'park' or leisure == 'garden' then
            tables.landuse:insert({
                name = tags.name,
                class = leisure,
                geom = object:as_multipolygon(),
            })
            return
        end

        -- Natural
        if natural_tag and natural_classes[natural_tag] then
            tables.natural:insert({
                name = tags.name,
                class = natural_tag,
                geom = object:as_multipolygon(),
            })
            return
        end
    end
end
