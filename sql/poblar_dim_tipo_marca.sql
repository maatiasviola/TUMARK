-- TIPOS MARCAS
INSERT INTO dim_tipo_marca (id_tipo_marca, tipo_marca) VALUES
(1, 'Combinacion de Colores'),
(2, 'Denominativa'),
(3, 'Secuencial'),
(4, 'Figurativa'),
(5, 'Gustativa'),
(6, 'Tactil'),
(7, 'Mixta'),
(8, 'Olfativa'),
(9, 'Posicion'),
(10, 'Tridimensional Mixta'),
(11, 'Sonora'),
(12, 'Tridimensional'),
(13, 'Documentacion Respaldatoria') -- Agregado por si acaso aparecía cortado en la imagen
ON CONFLICT (id_tipo_marca) DO NOTHING;

