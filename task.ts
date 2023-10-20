import fs from 'node:fs';
import { FeatureCollection } from 'geojson';
import { JSONSchema6 } from 'json-schema';
import ETL, {
    Event,
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
            const dumper = new EsriDump(String(layer.environment.ARCGIS_URL));
            const schema = await dumper.schema();

            console.error(JSON.stringify(schema));
            return schema;
        }
    }

    async control(): Promise<void> {
        const layer = await this.layer();

        if (!layer.environment.ARCGIS_URL) throw new Error('No ArcGIS_URL Provided');

        const config: EsriDumpConfigInput = {
            approach: EsriDumpConfigApproach.ITER,
            headers: {},
            params: {}
        };

        if (layer.environment.ARCGIS_TOKEN || (layer.environment.ARCGIS_PORTAL && layer.environment.ARCGIS_USERNAME && layer.environment.ARCGIS_PASSWORD)) {
            if (!layer.environment.ARCGIS_TOKEN || Number(layer.environment.ARCGIS_EXPIRES) < +new Date()  + 1000 * 60 * 60) {
                const res: any = await this.fetch('/api/sink/esri', 'POST', {
                    url: layer.environment.ARCGIS_PORTAL,
                    username: layer.environment.ARCGIS_USERNAME,
                    password: layer.environment.ARCGIS_PASSWORD
                });

                layer.environment.ARCGIS_TOKEN = res.token;
                layer.environment.ARCGIS_EXPIRES = res.expires;

                await this.fetch(`/api/layer/${layer.id}`, 'PATCH', {
                    environment: {
                        ARCGIS_PORTAL: layer.environment.ARCGIS_PORTAL,
                        ARCGIS_USERNAME: layer.environment.ARCGIS_USERNAME,
                        ARCGIS_PASSWORD: layer.environment.ARCGIS_PASSWORD,
                        ARCGIS_TOKEN: layer.environment.ARCGIS_TOKEN,
                        ARCGIS_EXPIRES: layer.environment.ARCGIS_EXPIRES,
                        ARCGIS_URL: layer.environment.ARCGIS_URL
                    }
                });
            }

            config.params.token = String(layer.environment.ARCGIS_TOKEN);
        }

        const dumper = new EsriDump(String(layer.environment.ARCGIS_URL), config);

        dumper.fetch();

        const fc: FeatureCollection = {
            type: 'FeatureCollection',
            features: []
        };

        await new Promise<void>((resolve, reject) => {
            dumper.on('feature', (feature) => {
                feature.id = `layer-${layer.id}-${feature.id}`

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
