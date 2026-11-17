import proj4 from 'proj4';
import type { Geometry, Point, Polyline, Polygon } from 'arcgis-rest-api';
import { geojsonToArcGIS } from '@terraformer/arcgis';
import { Static, Type, TSchema } from '@sinclair/typebox';
import { Value } from '@sinclair/typebox/value';
import type { Feature as GeoJSONFeature, Geometry as GeoJSONGeometry, Point as GeoJSONPoint, LineString, Polygon as GeoJSONPolygon } from 'geojson';
import type Lambda from 'aws-lambda';
import { Feature } from '@tak-ps/node-cot';
import ETL, { TaskLayer, Event, SchemaType, handler as internal, local, DataFlowType, InvocationType } from '@tak-ps/etl';
import EsriDump, {
    EsriDumpConfigInput,
    EsriDumpConfigApproach
} from 'esri-dump';

const IncomingInput = Type.Object({
    ARCGIS_URL: Type.String(),
    ARCGIS_QUERY: Type.Optional(Type.String()),
    ARCGIS_QUERY_STRATEGY: Type.String({
        enum: [ 'Query', 'QueryTopFeatures' ],
        default: 'Query'
    }),
    ARCGIS_PARAMS: Type.Optional(Type.Array(Type.Object({
        Key: Type.String(),
        Value: Type.String()
    }))),
    ARCGIS_PORTAL: Type.Optional(Type.String()),
    ARCGIS_USERNAME: Type.Optional(Type.String()),
    ARCGIS_PASSWORD: Type.Optional(Type.String()),
})

const OutgoingInput = Type.Object({
    ARCGIS_PORTAL: Type.String(),
    ARCGIS_USERNAME: Type.String(),
    ARCGIS_PASSWORD: Type.String(),
    ARCGIS_POINTS_URL: Type.Optional(Type.String()),
    ARCGIS_LINES_URL: Type.Optional(Type.String()),
    ARCGIS_POLYS_URL: Type.Optional(Type.String()),
    PRESERVE_HISTORY: Type.Boolean({
        default: false,
        description: 'If true, will not update existing features, but create new ones instead.'
    }),
});

const EphemeralStore = Type.Object({
    ARCGIS_TOKEN: Type.Optional(Type.String()),
    ARCGIS_EXPIRES: Type.Optional(Type.String()),
    ARCGIS_REFERER: Type.Optional(Type.String()),
});

const EsriError = Type.Object({
    message: Type.Optional(Type.String())
});

const EsriErrorResponse = Type.Object({
    error: EsriError
});

const EsriEditResult = Type.Object({
    error: Type.Optional(Type.Unknown())
});

const EsriEditResponse = Type.Object({
    addResults: Type.Optional(Type.Array(EsriEditResult)),
    updateResults: Type.Optional(Type.Array(EsriEditResult))
});

const EsriQueryResponse = Type.Object({
    features: Type.Optional(Type.Array(Type.Object({
        attributes: Type.Record(Type.String(), Type.Unknown())
    })))
});

type OutgoingFeature = Static<typeof Feature.Feature>;

function isObject(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null;
}

function isOutgoingRecord(value: unknown): value is { geojson: OutgoingFeature } {
    return isObject(value)
        && isObject(value.geojson)
        && typeof value.geojson.id === 'string'
        && isObject(value.geojson.geometry)
        && isObject(value.geojson.properties);
}

function isPoint(geom: Geometry): geom is Point {
    return 'x' in geom && 'y' in geom;
}

function isPolyline(geom: Geometry): geom is Polyline {
    return 'paths' in geom;
}

function isPolygon(geom: Geometry): geom is Polygon {
    return 'rings' in geom;
}

function toWebMercator(geojson: Parameters<typeof geojsonToArcGIS>[0]): Geometry {
    const geom = geojsonToArcGIS(geojson);

    if (isPoint(geom)) {
        const [x, y] = proj4('EPSG:4326', 'EPSG:3857', [ geom.x, geom.y ]);
        if (x === undefined || y === undefined) throw new Error('Incompatible Geometry');

        geom.x = x;
        geom.y = y;
    } else if (isPolyline(geom)) {
        geom.paths = geom.paths.map((path) => path.map((p) => proj4('EPSG:4326', 'EPSG:3857', p)));
    } else if (isPolygon(geom)) {
        geom.rings = geom.rings.map((ring) => ring.map((r) => proj4('EPSG:4326', 'EPSG:3857', r)));
    } else {
        throw new Error('Unsupported geometry type');
    }

    geom.spatialReference = {
        wkid: 102100,
        latestWkid: 3857
    };

    return geom;
}

function splitGeometry(geometry: GeoJSONGeometry): Array<GeoJSONPoint | LineString | GeoJSONPolygon> {
    if (geometry.type === 'MultiPoint') {
        return geometry.coordinates.map((coordinates) => ({ type: 'Point', coordinates }));
    }

    if (geometry.type === 'MultiLineString') {
        return geometry.coordinates.map((coordinates) => ({ type: 'LineString', coordinates }));
    }

    if (geometry.type === 'MultiPolygon') {
        return geometry.coordinates.map((coordinates) => ({ type: 'Polygon', coordinates }));
    }

    if (geometry.type === 'GeometryCollection') return [];

    return [ geometry ];
}

async function esriPost<T extends TSchema>(
    url: string,
    headers: Headers,
    params: Record<string, string>,
    schema: T
): Promise<Static<T>> {
    const res = await fetch(url, {
        method: 'POST',
        headers,
        body: new URLSearchParams({ f: 'json', ...params })
    });

    if (!res.ok) throw new Error(await res.text());

    const body: unknown = await res.json();

    if (process.env.DEBUG) console.error(url, 'Res:', JSON.stringify(body));

    if (Value.Check(EsriErrorResponse, body)) throw new Error(body.error.message ?? JSON.stringify(body.error));
    if (!Value.Check(schema, body)) throw new Error(`Unexpected response: ${JSON.stringify(body)}`);

    return body;
}

function assertEdits(results: Array<Static<typeof EsriEditResult>> | undefined): void {
    const failed = results?.find((result) => result.error !== undefined);
    if (failed) throw new Error(JSON.stringify(failed.error));
}

export default class Task extends ETL {
    static name = 'etl-arcgis';
    static flow = [ DataFlowType.Incoming, DataFlowType.Outgoing ];
    static invocation = [ InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Outgoing) {
            return type === SchemaType.Input ? OutgoingInput : Type.Object({});
        }

        if (type === SchemaType.Input) return IncomingInput;

        const task = new Task();
        const layer = await task.fetchLayer();
        if (!layer.incoming) return Type.Object({});

        const env = await this.env(IncomingInput);
        if (!env.ARCGIS_URL) return Type.Object({});

        const dumper = await task.dumper({
            approach: EsriDumpConfigApproach.ITER,
            headers: {},
            params: {}
        }, layer);

        return Type.Unsafe(await dumper.schema());
    }

    async auth(
        layer: Static<typeof TaskLayer>,
        flow: DataFlowType,
        env: {
            url: string,
            username: string
            password: string
        }

    ): Promise<void> {
        const ephem = await this.ephemeral(EphemeralStore, flow);

        if (
            !ephem.ARCGIS_TOKEN
            || !ephem.ARCGIS_REFERER
            || Number(ephem.ARCGIS_EXPIRES) < Date.now() + 1000 * 5
        ) {
            console.log('ok - POST /api/esri')
            const res: object = await this.fetch('/api/esri', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    url: env.url,
                    username: env.username,
                    password: env.password
                })
            });

            if ('auth' in res && isObject(res.auth)) {
                ephem.ARCGIS_TOKEN = String('token' in res.auth ? res.auth.token : '');
                ephem.ARCGIS_EXPIRES = String('expires' in res.auth ? res.auth.expires : '');
                ephem.ARCGIS_REFERER = String('referer' in res.auth ? res.auth.referer : '');

                console.log(`ok - PATCH http://localhost:5001/api/layer/${layer.id}`)
                await this.setEphemeral({
                    ARCGIS_TOKEN: ephem.ARCGIS_TOKEN,
                    ARCGIS_EXPIRES: ephem.ARCGIS_EXPIRES,
                    ARCGIS_REFERER: ephem.ARCGIS_REFERER,
                }, flow);
            }
        }
    }

    async update(flow: DataFlowType): Promise<void> {
        if (flow !== DataFlowType.Outgoing) return;

        const layer = await this.fetchLayer();
        const env = await this.env(OutgoingInput, flow);

        await this.auth(layer, flow, {
            url: env.ARCGIS_PORTAL,
            username: env.ARCGIS_USERNAME,
            password: env.ARCGIS_PASSWORD
        });

        console.log('update:', flow);
    }

    async outgoing(event: Lambda.SQSEvent): Promise<boolean> {
        const layer = await this.fetchLayer();
        const env = await this.env(OutgoingInput, DataFlowType.Outgoing);

        await this.auth(layer, DataFlowType.Outgoing, {
            url: env.ARCGIS_PORTAL,
            username: env.ARCGIS_USERNAME,
            password: env.ARCGIS_PASSWORD
        });

        const ephem = await this.ephemeral(EphemeralStore, DataFlowType.Outgoing);

        const headers = new Headers({
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-Esri-Authorization': `Bearer ${ephem.ARCGIS_TOKEN ?? ''}`
        });
        if (ephem.ARCGIS_REFERER) headers.set('Referer', ephem.ARCGIS_REFERER);

        await Promise.allSettled(event.Records.map(async (record) => {
            try {
                const parsed: unknown = JSON.parse(record.body);
                if (!isOutgoingRecord(parsed)) throw new Error('Record is not a GeoJSON Feature');

                return await Task.sync(parsed.geojson, env, headers);
            } catch (err) {
                console.error(err, 'Record:', record.body);
                return false;
            }
        }));

        return true;
    }

    static async sync(
        feat: OutgoingFeature,
        env: Static<typeof OutgoingInput>,
        headers: Headers
    ): Promise<boolean> {
        const urls: Record<string, string | undefined> = {
            Point: env.ARCGIS_POINTS_URL,
            LineString: env.ARCGIS_LINES_URL,
            Polygon: env.ARCGIS_POLYS_URL
        };

        const url = urls[feat.geometry.type];
        if (!url) {
            console.error(`ok - skipping ${feat.properties.callsign} due to geometry: ${feat.geometry.type}`);
            return false;
        }

        const geometry = toWebMercator(feat.geometry);

        const attributes = {
            cotuid: feat.id,
            callsign: feat.properties.callsign || 'Unknown',
            remarks: feat.properties.remarks || '',
            type: feat.properties.type,
            how: feat.properties.how,
            time: feat.properties.time,
            start: feat.properties.start,
            stale: feat.properties.stale
        };

        if (!env.PRESERVE_HISTORY) {
            const query = await esriPost(`${url}/query`, headers, {
                where: `cotuid='${feat.id.replaceAll("'", "''")}'`,
                outFields: '*'
            }, EsriQueryResponse);

            const existing = query.features?.[0];

            if (existing) {
                const body = await esriPost(`${url}/updateFeatures`, headers, {
                    features: JSON.stringify([{
                        attributes: {
                            ...attributes,
                            objectid: existing.attributes.objectid,
                            callsign: feat.properties.callsign
                        },
                        geometry
                    }])
                }, EsriEditResponse);

                assertEdits(body.updateResults);

                return true;
            }
        }

        const body = await esriPost(`${url}/addFeatures`, headers, {
            features: JSON.stringify([{ attributes, geometry }])
        }, EsriEditResponse);

        assertEdits(body.addResults);

        return true;
    }

    /**
     * Return a configured instance of ESRI Dump
     */
    async dumper(
        config: EsriDumpConfigInput,
        layer: Static<typeof  TaskLayer>
    ): Promise<EsriDump> {
        const env = await this.env(IncomingInput);
        const stored = await this.ephemeral(EphemeralStore, DataFlowType.Incoming);

        if (
            (stored.ARCGIS_TOKEN && stored.ARCGIS_EXPIRES)
            || (env.ARCGIS_USERNAME && env.ARCGIS_PASSWORD)
        ) {
            await this.auth(layer, DataFlowType.Incoming, {
                url: env.ARCGIS_PORTAL || env.ARCGIS_URL,
                username: env.ARCGIS_USERNAME || '',
                password: env.ARCGIS_PASSWORD || ''
            });

            const ephem = await this.ephemeral(EphemeralStore, DataFlowType.Incoming);

            if (ephem.ARCGIS_TOKEN) config.params = { ...config.params, token: ephem.ARCGIS_TOKEN };
            if (ephem.ARCGIS_REFERER) config.headers = { ...config.headers, Referer: ephem.ARCGIS_REFERER };
        }

        return new EsriDump(env.ARCGIS_URL, config);
    }

    async control(): Promise<void> {
        const layer = await this.fetchLayer();
        const env = await this.env(IncomingInput);

        if (!env.ARCGIS_URL) throw new Error('No ArcGIS_URL Provided');

        const params: Record<string, string> = {};

        if (env.ARCGIS_QUERY) params.where = env.ARCGIS_QUERY;

        for (const param of env.ARCGIS_PARAMS ?? []) {
            params[param.Key] = param.Value;
        }

        const dumper = await this.dumper({
            approach: env.ARCGIS_QUERY_STRATEGY === 'Query' ? EsriDumpConfigApproach.ITER : EsriDumpConfigApproach.TOP_FEATURES_ITER,
            headers: {},
            params
        }, layer);

        const fc: Static<typeof Feature.InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: []
        };

        await new Promise<void>((resolve, reject) => {
            dumper.on('feature', (feature: GeoJSONFeature) => {
                const id = `layer-${layer.id}-${String(feature.id)}`;
                const properties = { metadata: feature.properties ?? {} };
                const multi = feature.geometry.type.startsWith('Multi');

                splitGeometry(feature.geometry).forEach((geometry, idx) => {
                    fc.features.push({
                        id: multi ? `${id}-${idx}` : id,
                        type: 'Feature',
                        properties,
                        geometry
                    });
                });
            }).on('error', (err: unknown) => {
                reject(err instanceof Error ? err : new Error(String(err)));
            }).on('done', () => {
                resolve();
            });

            dumper.fetch().catch(reject);
        });

        console.log(`ok - obtained ${fc.features.length} features`);

        await this.submit(fc);
    }
}

await local(await Task.init(import.meta.url), import.meta.url);
export async function handler(event: Event = {}): ReturnType<typeof internal> {
    return internal(await Task.init(import.meta.url), event);
}

