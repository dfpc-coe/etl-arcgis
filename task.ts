import { Static, Type, TSchema } from '@sinclair/typebox';
import type { InputFeatureCollection } from '@tak-ps/etl'
import ETL, { TaskLayer, Event, SchemaType, handler as internal, local, DataFlowType, InvocationType } from '@tak-ps/etl';
import EsriDump, {
    EsriDumpConfigInput,
    EsriDumpConfigApproach
} from 'esri-dump';

const Input = Type.Object({
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

export default class Task extends ETL {
    static name = 'etl-arcgis';
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Webhook, InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming) {
            if (type === SchemaType.Input) {
                return {
                    type: 'object',
                    display: 'arcgis',
                    properties: {}
                } as unknown as TSchema;
            } else {
                const task = new Task();
                const layer = await task.fetchLayer();
                const env = await this.env(Input);

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
        } else {
            return Type.Object({});
        }
    }

    /**
     * Return a configured instance of ESRI Dump
     */
    async dumper(config: EsriDumpConfigInput, layer: Static<typeof  TaskLayer>): Promise<EsriDump> {
        const env = await this.env(Input);

        if (
            (layer.ephemeral.ARCGIS_TOKEN && layer.ephemeral.ARCGIS_EXPIRES)
            || (env.ARCGIS_USERNAME && env.ARCGIS_PASSWORD)
        ) {
            if (
                !layer.ephemeral.ARCGIS_TOKEN
                || !layer.ephemeral.ARCGIS_REFERER
                || Number(layer.ephemeral.ARCGIS_EXPIRES) < +new Date()  + 1000 * 5 // Token expires in under 5 minutes
            ) {
                console.log('ok - POST http://localhost:5001/api/esri')
                const res: object = await this.fetch('/api/esri', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        url: env.ARCGIS_PORTAL || env.ARCGIS_URL,
                        username: env.ARCGIS_USERNAME,
                        password: env.ARCGIS_PASSWORD
                    })
                });

                if ('auth' in res && typeof res.auth === 'object') {
                    layer.ephemeral.ARCGIS_TOKEN = String('token' in res.auth ? res.auth.token : '');
                    layer.ephemeral.ARCGIS_EXPIRES = String('expires' in res.auth ? res.auth.expires : '');
                    layer.ephemeral.ARCGIS_REFERER = String('referer' in res.auth ? res.auth.referer : '');

                    console.log(`ok - PATCH http://localhost:5001/api/layer/${layer.id}`)
                    await this.setEphemeral({
                        ARCGIS_TOKEN: layer.ephemeral.ARCGIS_TOKEN,
                        ARCGIS_EXPIRES: layer.ephemeral.ARCGIS_EXPIRES,
                        ARCGIS_REFERER: layer.ephemeral.ARCGIS_REFERER,
                    });
                }
            }

            config.params.token = layer.ephemeral.ARCGIS_TOKEN;
            config.headers.Referer = layer.ephemeral.ARCGIS_REFERER;
        }

        return new EsriDump(env.ARCGIS_URL, config);
    }

    async control(): Promise<void> {
        const layer = await this.fetchLayer();
        const env = await this.env(Input);

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

await local(new Task(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(import.meta.url), event);
}

