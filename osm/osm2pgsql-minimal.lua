-- Minimal OSM import for fast testing
-- Only imports roads, buildings, and water

local srid = 3857

local tables = {}

tables.roads = osm2pgsql.define_way_table('roads', {
    { column = 'name', type = 'text' },
    { column = 'class', type = 'text' },
    { column = 'geom', type = 'linestring', projection = srid, not_null = true },
}, { schema = 'osm' })

tables.buildings = osm2pgsql.define_area_table('buildings', {
    { column = 'name', type = 'text' },
    { column = 'geom', type = 'geometry', projection = srid, not_null = true },
}, { schema = 'osm' })

tables.water = osm2pgsql.define_area_table('water', {
    { column = 'name', type = 'text' },
    { column = 'geom', type = 'geometry', projection = srid, not_null = true },
}, { schema = 'osm' })

local road_classes = {
    motorway = 'motorway', motorway_link = 'motorway',
    trunk = 'trunk', trunk_link = 'trunk',
    primary = 'primary', primary_link = 'primary',
    secondary = 'secondary', secondary_link = 'secondary',
    tertiary = 'tertiary', tertiary_link = 'tertiary',
    residential = 'residential',
    unclassified = 'unclassified',
    service = 'service',
}

function osm2pgsql.process_way(object)
    local tags = object.tags

    local highway = tags.highway
    if highway and road_classes[highway] then
        tables.roads:insert({
            name = tags.name,
            class = road_classes[highway],
            geom = object:as_linestring(),
        })
        return
    end

    if not object.is_closed then
        return
    end

    if tags.building then
        tables.buildings:insert({
            name = tags.name,
            geom = object:as_polygon(),
        })
        return
    end

    if tags.natural == 'water' or tags.waterway == 'riverbank' then
        tables.water:insert({
            name = tags.name,
            geom = object:as_polygon(),
        })
        return
    end
end

function osm2pgsql.process_relation(object)
    local tags = object.tags
    if tags.type ~= 'multipolygon' then
        return
    end

    if tags.building then
        tables.buildings:insert({
            name = tags.name,
            geom = object:as_multipolygon(),
        })
        return
    end

    if tags.natural == 'water' then
        tables.water:insert({
            name = tags.name,
            geom = object:as_multipolygon(),
        })
    end
end
