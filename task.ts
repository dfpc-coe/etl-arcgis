import fs from 'node:fs';
import { FeatureCollection } from 'geojson';
import { JSONSchema6 } from 'json-schema';
import ETL, {
    Event,
    TaskLayer,
    SchemaType
} from '@tak-ps/etl';
import EsriDump, {
    EsriDumpConfigInput,
    EsriDumpConfigApproach
} from 'esri-dump';

try {
    const dotfile = new URL('.env', import.meta.url);

    fs.accessSync(dotfile);

    Object.assign(process.env, JSON.parse(String(fs.readFileSync(dotfile))));
} catch (err) {
    console.log('ok - no .env file loaded');
}

export default class Task extends ETL {
    static async schema(type: SchemaType = SchemaType.Input): Promise<JSONSchema6> {
        if (type === SchemaType.Input) {
            return {
                // @ts-ignore
                display: 'arcgis'
            };
        } else {
            const task = new Task();
            const layer = await task.layer();

            const config: EsriDumpConfigInput = {
                approach: EsriDumpConfigApproach.ITER,
                headers: {},
                params: {}
            };

            const dumper = await task.dumper(config, layer);
            const schema = await dumper.schema();

            return schema;
        }
    }

    /**
     * Return a configured instance of ESRI Dump
     */
    async dumper(config: EsriDumpConfigInput, layer: TaskLayer): Promise<EsriDump> {
        if (
            (layer.environment.ARCGIS_TOKEN && layer.environment.ARCGIS_EXPIRES)
            || (layer.environment.ARCGIS_PORTAL && layer.environment.ARCGIS_USERNAME && layer.environment.ARCGIS_PASSWORD)
        ) {
            if (!layer.environment.ARCGIS_TOKEN || Number(layer.environment.ARCGIS_EXPIRES) < +new Date()  + 1000 * 60 * 60) {
                const res: object = await this.fetch('/api/esri', 'POST', {
                    url: layer.environment.ARCGIS_PORTAL,
                    username: layer.environment.ARCGIS_USERNAME,
                    password: layer.environment.ARCGIS_PASSWORD
                });

                layer.environment.ARCGIS_TOKEN = String('token' in res ? res.token : '');
                layer.environment.ARCGIS_EXPIRES = String('expires' in res ? res.expires : '');

                await this.fetch(`/api/layer/${layer.id}`, 'PATCH', {
                    environment: {
                        ARCGIS_PORTAL: layer.environment.ARCGIS_PORTAL,
                        ARCGIS_USERNAME: layer.environment.ARCGIS_USERNAME,
                        ARCGIS_PASSWORD: layer.environment.ARCGIS_PASSWORD,
                        ARCGIS_TOKEN: layer.environment.ARCGIS_TOKEN,
                        ARCGIS_QUERY: layer.environment.ARCGIS_QUERY,
                        ARCGIS_EXPIRES: layer.environment.ARCGIS_EXPIRES,
                        ARCGIS_URL: layer.environment.ARCGIS_URL
                    }
                });
            }

            config.params.token = String(layer.environment.ARCGIS_TOKEN);
        }

        return new EsriDump(String(layer.environment.ARCGIS_URL), config);
    }

    async control(): Promise<void> {
        const layer = await this.layer();

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

        let count = 0;
        await new Promise<void>((resolve, reject) => {
            dumper.on('feature', (feature) => {
                feature.id = `layer-${layer.id}-${feature.id}`
                ++count;

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

        console.log(`ok - obtained ${count} features`);

        await this.submit(fc);
    }
}

export async function handler(event: Event = {}) {
    if (event.type === 'schema:input') {
        return await Task.schema(SchemaType.Input);
    } else if (event.type === 'schema:output') {
        return await Task.schema(SchemaType.Output);
    } else {
        const task = new Task();
        await task.control();
    }
}

if (import.meta.url === `file://${process.argv[1]}`) handler();
