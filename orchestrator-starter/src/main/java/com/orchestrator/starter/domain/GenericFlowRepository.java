package com.orchestrator.starter.domain;

import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.repository.query.FluentQuery;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Generic repository using MongoTemplate. Auto-registered when the user
 * doesn't define their own OrchestratorFlowRepository.
 *
 * Implements only the methods the library actually uses.
 * Other MongoRepository methods throw UnsupportedOperationException.
 */
@RequiredArgsConstructor
public class GenericFlowRepository<F extends OrchestratorFlow> implements OrchestratorFlowRepository<F> {

    private final MongoTemplate mongoTemplate;
    private final Class<F> entityClass;

    @Override
    public <S extends F> S save(S entity) {
        return mongoTemplate.save(entity);
    }

    @Override
    public Optional<F> findById(String id) {
        return Optional.ofNullable(mongoTemplate.findById(id, entityClass));
    }

    @Override
    public boolean existsById(String id) {
        return findById(id).isPresent();
    }

    @Override
    public Optional<F> findByCorrelationId(String correlationId) {
        Query query = new Query(Criteria.where("correlationId").is(correlationId));
        return Optional.ofNullable(mongoTemplate.findOne(query, entityClass));
    }

    @Override
    public List<F> findByStatusAndUpdatedAtBefore(FlowStatus status, Instant threshold) {
        Query query = new Query(Criteria.where("status").is(status)
                .and("updatedAt").lt(threshold));
        return mongoTemplate.find(query, entityClass);
    }

    @Override
    public List<F> findByStatus(FlowStatus status) {
        Query query = new Query(Criteria.where("status").is(status));
        return mongoTemplate.find(query, entityClass);
    }

    @Override
    public List<F> findAll() {
        return mongoTemplate.findAll(entityClass);
    }

    @Override
    public long count() {
        return mongoTemplate.count(new Query(), entityClass);
    }

    @Override
    public void deleteById(String id) {
        mongoTemplate.remove(new Query(Criteria.where("_id").is(id)), entityClass);
    }

    @Override
    public void delete(F entity) {
        mongoTemplate.remove(entity);
    }

    // ========== Methods the library doesn't use — required by MongoRepository interface ==========

    @Override public <S extends F> List<S> saveAll(Iterable<S> entities) { throw new UnsupportedOperationException(); }
    @Override public List<F> findAllById(Iterable<String> ids) { throw new UnsupportedOperationException(); }
    @Override public void deleteAllById(Iterable<? extends String> ids) { throw new UnsupportedOperationException(); }
    @Override public void deleteAll(Iterable<? extends F> entities) { throw new UnsupportedOperationException(); }
    @Override public void deleteAll() { throw new UnsupportedOperationException(); }
    @Override public List<F> findAll(Sort sort) { throw new UnsupportedOperationException(); }
    @Override public Page<F> findAll(Pageable pageable) { throw new UnsupportedOperationException(); }
    @Override public <S extends F> S insert(S entity) { throw new UnsupportedOperationException(); }
    @Override public <S extends F> List<S> insert(Iterable<S> entities) { throw new UnsupportedOperationException(); }
    @Override public <S extends F> Optional<S> findOne(Example<S> example) { throw new UnsupportedOperationException(); }
    @Override public <S extends F> List<S> findAll(Example<S> example) { throw new UnsupportedOperationException(); }
    @Override public <S extends F> List<S> findAll(Example<S> example, Sort sort) { throw new UnsupportedOperationException(); }
    @Override public <S extends F> Page<S> findAll(Example<S> example, Pageable pageable) { throw new UnsupportedOperationException(); }
    @Override public <S extends F> long count(Example<S> example) { throw new UnsupportedOperationException(); }
    @Override public <S extends F> boolean exists(Example<S> example) { throw new UnsupportedOperationException(); }
    @Override public <S extends F, R> R findBy(Example<S> example, Function<FluentQuery.FetchableFluentQuery<S>, R> queryFunction) { throw new UnsupportedOperationException(); }
}
