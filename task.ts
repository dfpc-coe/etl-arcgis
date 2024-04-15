import { FeatureCollection } from 'geojson';
import { TSchema } from '@sinclair/typebox';
import ETL, { TaskLayer, Event, SchemaType, handler as internal, local, env } from '@tak-ps/etl';
import EsriDump, {
    EsriDumpConfigInput,
    EsriDumpConfigApproach
} from 'esri-dump';

export default class Task extends ETL {
    async schema(type: SchemaType = SchemaType.Input): Promise<TSchema> {
        if (type === SchemaType.Input) {
            return {
                type: 'object',
                display: 'arcgis',
                properties: {}
            } as unknown as TSchema;
        } else {
            const task = new Task();
            const layer = await task.fetchLayer();

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

    /**
     * Return a configured instance of ESRI Dump
     */
    async dumper(config: EsriDumpConfigInput, layer: TaskLayer): Promise<EsriDump> {
        if (
            (layer.environment.ARCGIS_TOKEN && layer.environment.ARCGIS_EXPIRES)
            || (layer.environment.ARCGIS_USERNAME && layer.environment.ARCGIS_PASSWORD)
        ) {
            delete layer.environment.ARCGIS_REFERER;

            if (
                !layer.environment.ARCGIS_TOKEN
                || !layer.environment.ARCGIS_REFERER
                || Number(layer.environment.ARCGIS_EXPIRES) < +new Date()  + 1000 * 60 * 60
            ) {
                console.log('ok - POST http://localhost:5001/api/esri')
                const res: object = await this.fetch('/api/esri', 'POST', {
                    url: layer.environment.ARCGIS_PORTAL || layer.environment.ARCGIS_URL,
                    username: layer.environment.ARCGIS_USERNAME,
                    password: layer.environment.ARCGIS_PASSWORD
                });

                if ('auth' in res && typeof res.auth === 'object') {
                    res.auth as object;

                    layer.environment.ARCGIS_TOKEN = String('token' in res.auth ? res.auth.token : '');
                    layer.environment.ARCGIS_EXPIRES = String('expires' in res.auth ? res.auth.expires : '');
                    layer.environment.ARCGIS_REFERER = String('referer' in res.auth ? res.auth.referer : '');
                }

                console.log(`ok - PATCH http://localhost:5001/api/layer/${layer.id}`)
                await this.fetch(`/api/layer/${layer.id}`, 'PATCH', {
                    environment: {
                        ARCGIS_PORTAL: layer.environment.ARCGIS_PORTAL,
                        ARCGIS_USERNAME: layer.environment.ARCGIS_USERNAME,
                        ARCGIS_PASSWORD: layer.environment.ARCGIS_PASSWORD,
                        ARCGIS_QUERY: layer.environment.ARCGIS_QUERY,
                        ARCGIS_URL: layer.environment.ARCGIS_URL,
                        ARCGIS_TOKEN: layer.environment.ARCGIS_TOKEN,
                        ARCGIS_EXPIRES: layer.environment.ARCGIS_EXPIRES,
                        ARCGIS_REFERER: layer.environment.ARCGIS_REFERER,
                    }
                });
            }

            config.params.token = String(layer.environment.ARCGIS_TOKEN);
            config.headers.Referer = String(layer.environment.ARCGIS_REFERER);
        }

        return new EsriDump(String(layer.environment.ARCGIS_URL), config);
    }

    async control(): Promise<void> {
        const layer = await this.fetchLayer();

        if (!layer.environment.ARCGIS_URL) throw new Error('No ArcGIS_URL Provided');

        const config: EsriDumpConfigInput = {
            approach: EsriDumpConfigApproach.ITER,
            headers: {},
            params: {}
        };

        if (layer.environment.ARCGIS_QUERY) {
            config.params.where = String(layer.environment.ARCGIS_QUERY);
        }

        const dumper = await this.dumper(config, layer);

        dumper.fetch();

        const fc: FeatureCollection = {
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

env(import.meta.url)
await local(new Task(), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(), event);
}
 
