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
                type: 'object',
                required: ['ARCGIS_URL'],
                properties: {
                    'ARCGIS_URL': {
                        type: 'string',
                        description: 'ArcGIS MapServer URL to pull data from'
                    },
                    'ARCGIS_HEADERS': {
                        type: 'array',
                        description: 'Headers to include in the request',
                        items: {
                            type: 'object',
                            required: [
                                'key',
                                'value'
                            ],
                            properties: {
                                key: {
                                    type: 'string'
                                },
                                value: {
                                    type: 'string'
                                }
                            }
                        }
                    },
                    'ARCGIS_PARAMS': {
                        type: 'array',
                        description: 'URL Params to include in the request',
                        items: {
                            type: 'object',
                            required: [
                                'key',
                                'value'
                            ],
                            properties: {
                                key: {
                                    type: 'string'
                                },
                                value: {
                                    type: 'string'
                                }
                            }
                        }
                    },
                    'DEBUG': {
                        type: 'boolean',
                        default: false,
                        description: 'Print ADSBX results in logs'
                    }
                }
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

        if (!layer.environment.ARCGIS_HEADERS) layer.environment.ARCGIS_HEADERS = [];
        if (!layer.environment.ARCGIS_PARAMS) layer.environment.ARCGIS_PARAMS = [];

        if (!Array.isArray(layer.environment.ARCGIS_HEADERS)) throw new Error('ArcGIS_HEADERS must be an Array');
        if (!Array.isArray(layer.environment.ARCGIS_PARAMS)) throw new Error('ArcGIS_PARAMS must be an Array');

        const config: EsriDumpConfigInput = {
            approach: EsriDumpConfigApproach.ITER,
            headers: {},
            params: {}
        };

        for (const header of layer.environment.ARCGIS_HEADERS) {
            if (!header.name.trim()) continue;
            config.headers[header.name] = header.value || '';
        }
        for (const param of layer.environment.ARCGIS_PARAMS) {
            if (!param.name.trim()) continue;
            config.headers[param.name] = param.value || '';
        }

        const dumper = new EsriDump(String(layer.environment.ARCGIS_URL), config);

        dumper.fetch();

        const fc: FeatureCollection = {
            type: 'FeatureCollection',
            features: []
        };

        await new Promise<void>((resolve, reject) => {
            dumper.on('feature', (feature) => {
                fc.features.push(feature);
            }).on('error', (err) => {
                reject(err);
            }).on('done', () => {
                return resolve();
            });
        });

        await this.submit(fc);
    }
}

export async function handler(event: Event = {}) {
    if (event.type === 'schema:input') {
        return Task.schema(SchemaType.Input);
    } else if (event.type === 'schema:output') {
        return Task.schema(SchemaType.Output);
    } else {
        const task = new Task();
        await task.control();
    }
}

if (import.meta.url === `file://${process.argv[1]}`) handler();
