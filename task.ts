import proj4 from 'proj4';
import type { Geometry, Point, Polyline, Polygon } from 'arcgis-rest-api';
import { geojsonToArcGIS } from '@terraformer/arcgis';
import { Static, Type, TSchema } from '@sinclair/typebox';
import type Lambda from 'aws-lambda';
import { Feature } from '@tak-ps/node-cot';
import { InputFeatureCollection } from '@tak-ps/etl'
import ETL, { TaskLayer, Event, SchemaType, handler as internal, local, DataFlowType, InvocationType } from '@tak-ps/etl';
import EsriDump, {
    EsriDumpConfigInput,
    EsriDumpConfigApproach
} from 'esri-dump';

const IncomingInput = Type.Object({
    ARCGIS_URL: Type.String(),
    ARCGIS_QUERY: Type.Optional(Type.String()),
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

export default class Task extends ETL {
    static name = 'etl-arcgis';
    static flow = [ DataFlowType.Incoming, DataFlowType.Outgoing ];
    static invocation = [ InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming && type === SchemaType.Input) {
            return IncomingInput;
        } else if (flow === DataFlowType.Incoming && type === SchemaType.Output) {
            const task = new Task();
            const layer = await task.fetchLayer();

            if (!layer.incoming) {
                return Type.Object({});
            } else {
                const env = await this.env(IncomingInput);

                if (!env.ARCGIS_URL) {
                    return Type.Object({});
                } else {
                    const config: EsriDumpConfigInput = {
                        approach: EsriDumpConfigApproach.ITER,
                        headers: {},
                        params: {}
                    };

                    const dumper = await task.dumper(config, layer);
                    const schema = await dumper.schema();

                    return schema as TSchema;
                }
            }
        } else if (flow === DataFlowType.Outgoing && type === SchemaType.Input) {
            return OutgoingInput;
        } else if (flow === DataFlowType.Outgoing && type === SchemaType.Output) {
            return Type.Object({});
        }
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
        if (
            !layer[flow].ephemeral.ARCGIS_TOKEN
            || !layer[flow].ephemeral.ARCGIS_REFERER
            || Number(layer[flow].ephemeral.ARCGIS_EXPIRES) < +new Date()  + 1000 * 5 // Token expires in under 5 minutes
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

            if ('auth' in res && typeof res.auth === 'object') {
                layer[flow].ephemeral.ARCGIS_TOKEN = String('token' in res.auth ? res.auth.token : '');
                layer[flow].ephemeral.ARCGIS_EXPIRES = String('expires' in res.auth ? res.auth.expires : '');
                layer[flow].ephemeral.ARCGIS_REFERER = String('referer' in res.auth ? res.auth.referer : '');

                console.log(`ok - PATCH http://localhost:5001/api/layer/${layer.id}`)
                await this.setEphemeral({
                    ARCGIS_TOKEN: layer[flow].ephemeral.ARCGIS_TOKEN,
                    ARCGIS_EXPIRES: layer[flow].ephemeral.ARCGIS_EXPIRES,
                    ARCGIS_REFERER: layer[flow].ephemeral.ARCGIS_REFERER,
                }, flow);
            }
        }
    }

    async update(flow: DataFlowType): Promise<void> {
        const layer = await this.fetchLayer();

        if (flow === DataFlowType.Outgoing) {
            const env = await this.env(OutgoingInput, flow);

            await this.auth(layer, flow, {
                url: env.ARCGIS_PORTAL,
                username: env.ARCGIS_USERNAME,
                password: env.ARCGIS_PASSWORD
            });

            console.log('update:', flow);
        } else {
            return;
        }
    }

    async outgoing(event: Lambda.SQSEvent): Promise<boolean> {
        const layer = await this.fetchLayer();
        const env = await this.env(OutgoingInput, DataFlowType.Outgoing);

        const pool: Array<Promise<unknown>> = [];

        await this.auth(layer, DataFlowType.Outgoing, {
            url: env.ARCGIS_PORTAL,
            username: env.ARCGIS_USERNAME,
            password: env.ARCGIS_PASSWORD
        });

        for (const record of event.Records) {
            pool.push(
                (async (record: Lambda.SQSRecord) => {
                    try {
                        const feat = (JSON.parse(record.body) as {
                            xml: string,
                            geojson: Static<typeof Feature.Feature>
                        }).geojson;

                        let esriLayerURL;
                        if (env.ARCGIS_POINTS_URL && feat.geometry.type === 'Point') {
                            esriLayerURL = env.ARCGIS_POINTS_URL;
                        } else if (env.ARCGIS_LINES_URL && feat.geometry.type === 'LineString') {
                            esriLayerURL = env.ARCGIS_LINES_URL;
                        } else if (env.ARCGIS_POLYS_URL && feat.geometry.type === 'Polygon') {
                            esriLayerURL = env.ARCGIS_POLYS_URL;
                        } else {
                            console.error(`ok - skipping ${feat.properties.callsign} due to geometry: ${feat.geometry.type}`);
                            return false;
                        }

                        let geometry: Geometry;
                        if (feat.geometry.type === 'Point') {
                            const geom = geojsonToArcGIS(feat.geometry) as Point;
                            if (geom.x === undefined || geom.y === undefined) throw new Error('Incompatible Geometry');

                            const proj = proj4('EPSG:4326', 'EPSG:3857', [ geom.x, geom.y ]);

                            geom.x = proj[0];
                            geom.y = proj[1];

                            geometry = geom;
                        } else if (feat.geometry.type === 'LineString') {
                            const geom = geojsonToArcGIS(feat.geometry) as Polyline;

                            geom.paths = geom.paths.map((paths) => {
                                return paths.map((p) => {
                                    return proj4('EPSG:4326', 'EPSG:3857', p);
                                })
                            })

                            geometry = geom;
                        } else if (feat.geometry.type === 'Polygon') {
                            const geom = geojsonToArcGIS(feat.geometry) as Polygon;

                            geom.rings = geom.rings.map((ring) => {
                                return ring.map((r) => {
                                    return proj4('EPSG:4326', 'EPSG:3857', r);
                                })
                            })

                            geometry = geom;
                        }

                        geometry.spatialReference = {
                            wkid: 102100,
                            latestWkid: 3857
                        }

                        if (env.PRESERVE_HISTORY) {
                            const res = await fetch(new URL(esriLayerURL + '/addFeatures'), {
                                method: 'POST',
                                headers: {
                                    'Referer': layer.outgoing.ephemeral.ARCGIS_REFERER,
                                    'Content-Type': 'application/x-www-form-urlencoded',
                                    'X-Esri-Authorization': `Bearer ${layer.outgoing.ephemeral.ARCGIS_TOKEN}`
                                },
                                body: new URLSearchParams({
                                    'f': 'json',
                                    'features': JSON.stringify([{
                                        attributes: {
                                            cotuid: feat.id,
                                            callsign: feat.properties.callsign || 'Unknown',
                                            remarks: feat.properties.remarks || '',
                                            type: feat.properties.type,
                                            how: feat.properties.how,
                                            time: feat.properties.time,
                                            start: feat.properties.start,
                                            stale: feat.properties.stale
                                        },
                                        geometry
                                    }])
                                })
                            });

                            if (!res.ok) throw new Error(await res.text());

                            const body = await res.json();

                            if (process.env.DEBUG) console.error('/addFeatures', feat.properties.callsign, 'Res:', JSON.stringify(body));

                            if (body.addResults.length && body.addResults[0].error) throw new Error(JSON.stringify(body.addResults[0].error));

                            return true;
                        } else {
                            const res_query = await fetch(esriLayerURL + '/query', {
                                method: 'POST',
                                headers: {
                                    'Referer': layer.outgoing.ephemeral.ARCGIS_REFERER,
                                    'Content-Type': 'application/x-www-form-urlencoded',
                                    'X-Esri-Authorization': `Bearer ${layer.outgoing.ephemeral.ARCGIS_TOKEN}`
                                },
                                body: new URLSearchParams({
                                    'f': 'json',
                                    'where': `cotuid='${feat.id}'`,
                                        'outFields': '*'
                                })
                            });

                            if (!res_query.ok) throw new Error(await res_query.text());
                            const query = await res_query.json();

                            if (process.env.DEBUG) console.error('/query', feat.properties.callsign, 'Res:', JSON.stringify(query));

                            if (query.error) throw new Error(query.error.message);

                            if (!query.features.length) {
                                const res = await fetch(new URL(esriLayerURL + '/addFeatures'), {
                                    method: 'POST',
                                    headers: {
                                        'Referer': layer.outgoing.ephemeral.ARCGIS_REFERER,
                                        'Content-Type': 'application/x-www-form-urlencoded',
                                        'X-Esri-Authorization': `Bearer ${layer.outgoing.ephemeral.ARCGIS_TOKEN}`
                                    },
                                    body: new URLSearchParams({
                                        'f': 'json',
                                        'features': JSON.stringify([{
                                            attributes: {
                                                cotuid: feat.id,
                                                callsign: feat.properties.callsign || 'Unknown',
                                                remarks: feat.properties.remarks || '',
                                                type: feat.properties.type,
                                                how: feat.properties.how,
                                                time: feat.properties.time,
                                                start: feat.properties.start,
                                                stale: feat.properties.stale
                                            },
                                            geometry
                                        }])
                                    })
                                });

                                if (!res.ok) throw new Error(await res.text());

                                const body = await res.json();

                                if (process.env.DEBUG) console.error('/addFeatures', feat.properties.callsign, 'Res:', JSON.stringify(body));

                                if (body.addResults.length && body.addResults[0].error) throw new Error(JSON.stringify(body.addResults[0].error));

                                return true;
                            } else {
                                const oid = query.features[0].attributes.objectid;

                                const res = await fetch(new URL(esriLayerURL + '/updateFeatures'), {
                                    method: 'POST',
                                    headers: {
                                        'Referer': layer.outgoing.ephemeral.ARCGIS_REFERER,
                                        'Content-Type': 'application/x-www-form-urlencoded',
                                        'X-Esri-Authorization': `Bearer ${layer.outgoing.ephemeral.ARCGIS_TOKEN}`
                                    },
                                    body: new URLSearchParams({
                                        'f': 'json',
                                        'features': JSON.stringify([{
                                            attributes: {
                                                objectid: oid,
                                                cotuid: feat.id,
                                                callsign: feat.properties.callsign,
                                                type: feat.properties.type,
                                                how: feat.properties.how,
                                                time: feat.properties.time,
                                                start: feat.properties.start,
                                                stale: feat.properties.stale
                                            },
                                            geometry
                                        }])
                                    })
                                });

                                if (!res.ok) throw new Error(await res.text());

                                const body = await res.json();

                                if (process.env.DEBUG) console.error('/updateFeatures', feat.properties.callsign, 'Res:', JSON.stringify(body));

                                if (body.updateResults.length && body.updateResults[0].error) throw new Error(JSON.stringify(body.updateResults[0].error));

                                return true;
                            }
                        }
                    } catch (err) {
                        console.error(err, 'Record:', record.body);
                    }
                })(record)
            )
        }

        await Promise.allSettled(pool);

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

        if (
            (layer.incoming.ephemeral.ARCGIS_TOKEN && layer.incoming.ephemeral.ARCGIS_EXPIRES)
            || (env.ARCGIS_USERNAME && env.ARCGIS_PASSWORD)
        ) {
            this.auth(layer, DataFlowType.Incoming, {
                url: env.ARCGIS_PORTAL || env.ARCGIS_URL,
                username: env.ARCGIS_USERNAME,
                password: env.ARCGIS_PASSWORD
            })

            config.params.token = layer.incoming.ephemeral.ARCGIS_TOKEN;
            config.headers.Referer = layer.incoming.ephemeral.ARCGIS_REFERER;
        }

        return new EsriDump(env.ARCGIS_URL, config);
    }

    async control(): Promise<void> {
        const layer = await this.fetchLayer();
        const env = await this.env(IncomingInput);

        if (!env.ARCGIS_URL) throw new Error('No ArcGIS_URL Provided');

        const config: EsriDumpConfigInput = {
            approach: EsriDumpConfigApproach.ITER,
            headers: {},
            params: {}
        };

        if (env.ARCGIS_QUERY) {
            config.params.where = env.ARCGIS_QUERY;
        }

        if (env.ARCGIS_PARAMS && env.ARCGIS_PARAMS.length) {
            for (const param of env.ARCGIS_PARAMS) {
                config.params[param.Key] = param.Value;
            }
        }

        const dumper = await this.dumper(config, layer);

        dumper.fetch();

        const fc: Static<typeof InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: []
        };

        await new Promise<void>((resolve, reject) => {
            dumper.on('feature', (feature) => {
                feature.id = `layer-${layer.id}-${feature.id}`

                feature.properties = {
                    metadata: feature.properties
                };

                if (feature.geometry.type.startsWith('Multi')) {
                    feature.geometry.coordinates.forEach((coords: any, idx: number) => {
                        fc.features.push({
                            id: feature.id + '-' + idx,
                            type: 'Feature',
                            properties: feature.properties,
                            geometry: {
                                type: feature.geometry.type.replace('Multi', ''),
                                coordinates: coords
                            }
                        });
                    });
                } else {
                    fc.features.push(feature)
                }
            }).on('error', (err) => {
                reject(err);
            }).on('done', () => {
                return resolve();
            });
        });

        console.log(`ok - obtained ${fc.features.length} features`);

        await this.submit(fc);
    }
}

await local(await Task.init(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(await Task.init(import.meta.url), event);
}

