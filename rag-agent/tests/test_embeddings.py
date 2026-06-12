import numpy as np
from rag_agent.embeddings import EmbeddingGenerator, embedding_generator


class TestEmbeddingGenerator:

    def test_embedding_generator_init(self):
        generator = EmbeddingGenerator()
        assert generator.model_name == "all-MiniLM-L6-v2"
        assert generator.model is None

    def test_load_model(self):
        generator = EmbeddingGenerator()
        generator.load()
        assert generator.model is not None

    def test_encode_single_text(self):
        generator = EmbeddingGenerator()
        generator.load()

        vector = generator.encode("test text")

        assert isinstance(vector, list)
        assert len(vector) == 384
        assert all(isinstance(x, float) for x in vector)

    def test_encode_batch(self):
        generator = EmbeddingGenerator()
        generator.load()

        texts = ["first text", "second text", "third text"]
        vectors = generator.encode_batch(texts)

        assert isinstance(vectors, list)
        assert len(vectors) == 3
        assert all(len(v) == 384 for v in vectors)

    def test_normalized_vectors(self):
        generator = EmbeddingGenerator()
        generator.load()

        vector = generator.encode("test text")
        vector_np = np.array(vector)

        norm = np.linalg.norm(vector_np)
        assert abs(norm - 1.0) < 1e-6

    def test_similar_texts_produce_similar_vectors(self):
        generator = EmbeddingGenerator()
        generator.load()

        text1 = "route from station to airport"
        text2 = "how to get from train station to airport"
        text3 = "weather forecast for tomorrow"

        vec1 = np.array(generator.encode(text1))
        vec2 = np.array(generator.encode(text2))
        vec3 = np.array(generator.encode(text3))

        sim_similar = np.dot(vec1, vec2)
        sim_different = np.dot(vec1, vec3)

        assert sim_similar > sim_different

    def test_global_embedding_generator(self):
        assert embedding_generator is not None
        assert isinstance(embedding_generator, EmbeddingGenerator)
