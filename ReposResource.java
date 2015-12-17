/*
 * Copyright (c) 2015 phrebejk.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *    phrebejk - initial API and implementation and/or initial documentation
 */
package com.tasktop.c2c.server.scm.service.git.repos;

import com.tasktop.c2c.server.common.service.AbstactServiceBean;
import com.tasktop.c2c.server.common.service.EntityNotFoundException;
import com.tasktop.c2c.server.common.service.ValidationException;
import com.tasktop.c2c.server.common.service.domain.Role;
import com.tasktop.c2c.server.common.service.web.spring.TenancyUtil;
import com.tasktop.c2c.server.scm.domain.Commit;
import com.tasktop.c2c.server.scm.domain.Import;
import com.tasktop.c2c.server.scm.domain.InitRepositoryRequest;
import com.tasktop.c2c.server.scm.domain.ScmLocation;
import com.tasktop.c2c.server.scm.domain.ScmRepository;
import com.tasktop.c2c.server.scm.domain.ScmType;
import com.tasktop.c2c.server.scm.service.Despringifier;
import com.tasktop.c2c.server.scm.service.EventGeneratingPostRecieveHook;
import com.tasktop.c2c.server.scm.service.GitCommit;
import com.tasktop.c2c.server.scm.service.GitRepositories;
import com.tasktop.c2c.server.scm.service.ScmServiceConfiguration;
import com.tasktop.c2c.server.scm.service.git.SingleRepositoryResource;
import com.tasktop.c2c.server.scm.service.git.search.SearchResource;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.validator.UrlValidator;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.JGitInternalException;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;
import org.eclipse.jgit.internal.storage.dfs.InMemoryRepository;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.transport.RemoteConfig;
import org.eclipse.jgit.transport.URIish;
import org.eclipse.jgit.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.annotation.Secured;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

/**
 * Repositories maintenance create/delete/list
 *
 * @author phrebejk
 */
public class ReposResource extends SingleRepositoryResource {

    private final ScmServiceConfiguration profileServiceConfiguration = Despringifier.getInstance().getScmServiceConfiguration();
    
    private static final Logger LOG = LoggerFactory.getLogger(ReposResource.class.getSimpleName());

    private static final Validation VALIDATION = new Validation();

    private static final RepositoryInfoCache repositoryInfoCache = Despringifier.getInstance().getRepositoryInfoCache();

    public ReposResource() {
        super();
    }

    public ReposResource( String repoName ) throws EntityNotFoundException {
        super( repoName );
    }

    @Secured(Role.Admin)
    public ScmRepository create(ScmRepository repository) throws EntityNotFoundException, ValidationException {

        String internalUrlPrefix = profileServiceConfiguration.getHostedScmUrlPrefix(TenancyUtil
                .getCurrentTenantProjectIdentifer());

        // Provide Defaults
        if ( ScmLocation.EXTERNAL == repository.getScmLocation() ) {
            if ( repository.getUrl() == null ) {
                throw new ValidationException("External URL is null");
            }
            repository.setName(getRepoDirNameFromExternalUrl(repository.getUrl()));
        }
        else if (ScmLocation.CODE2CLOUD == repository.getScmLocation() && repository.getName() != null) {
            if (!repository.getName().endsWith(".git")) {
                Errors errors = VALIDATION.createErrors(repository);
                errors.reject("scmrepo.internal.nameMustEndWithGit");
                VALIDATION.throwValidationException(errors);
            } else if (repository.getName().equals(".git")) {
                Errors errors = VALIDATION.createErrors(repository);
                errors.reject("scmrepo.internal.nameEmpty");
                VALIDATION.throwValidationException(errors);
            }
            repository.setUrl(internalUrlPrefix + repository.getName());
            repository.setAlternateUrl(profileServiceConfiguration.computeSshUrl(repository));
        }

        // Test that repo does not exists
        if ( repositoryProvider.getRepositoryLocation(repository.getName()) != null ) {
            Errors errors = VALIDATION.createErrors(repository);
            errors.reject("scmrepo.urlExists");
            VALIDATION.throwValidationException(errors);
        }

        // Validate the internal object.
        VALIDATION.validate(repository, VALIDATION.getValidator(), new ScmExternalRepositoryValidator());
        
        if (ScmLocation.EXTERNAL.equals(repository.getScmLocation())
                && repository.getUrl().startsWith(internalUrlPrefix)) {
            Errors errors = VALIDATION.createErrors(repository);
            errors.reject("scmrepo.external.url.isInternal");
            VALIDATION.throwValidationException(errors);
        }
        if (ScmLocation.CODE2CLOUD.equals(repository.getScmLocation())
                && !repository.getUrl().startsWith(internalUrlPrefix)) {
            Errors errors = VALIDATION.createErrors(repository);
            errors.reject("scmrepo.internal.url.isExternal");
            VALIDATION.throwValidationException(errors);
        }
        if (!ScmType.GIT.equals(repository.getType())) {
            throw new IllegalStateException("only git repos supported");
        }

        if (repository.getScmLocation().equals(ScmLocation.EXTERNAL)) {
            GitRepositories.removeRepoFromCache(repositoryProvider.getTenantMirroredBaseDir(), repository.getName());
            Repository gitRepo = addExternalRepository(repository.getUrl());            
            Import.Request request = new Import.Request();
            request.setDryRun(false);
            request.setUrl(repository.getUrl());
            try {
                Import.Status importStatus = Importer.importRepository(gitRepo, request);
                repository.setImportInProgress(Import.Status.State.IMPORTING.equals(importStatus.state));
            } catch (IOException ex) {
                throw new EntityNotFoundException();
            }
        } else {
            try {
                GitRepositories.removeRepoFromCache(repositoryProvider.getTenantHostedBaseDir(), repository.getName());
                GitRepositories.createEmptyRepository(repositoryProvider.getTenantHostedBaseDir(), repository.getName());
            }
            catch( oracle.clouddev.server.common.client.api.exception.ValidationException ex ) {
                throw new ValidationException( ex.getMessage() );
            }
        }

        setRepositoryDescription(repository.getName(), repository.getDescription(), repository.getScmLocation());

        return repository;
    }

    public void delete(String repoName) throws EntityNotFoundException {

        if ( repoName == null ) {
            throw new EntityNotFoundException( "null repo name" );
        }

        findRepositoryByName(repoName); // ENFE if does not exist

        ScmLocation location = repositoryProvider.getRepositoryLocation(repoName);

        if (ScmLocation.EXTERNAL.equals(location)) {
            GitRepositories.removeRepoFromCache(repositoryProvider.getTenantMirroredBaseDir(), repoName);
            removeRepository(repositoryProvider.getTenantMirroredBaseDir(), repoName);
        } else {
            GitRepositories.removeRepoFromCache(repositoryProvider.getTenantHostedBaseDir(), repoName);
            removeRepository(repositoryProvider.getTenantHostedBaseDir(), repoName);
        }
    }

    public void setRepositoryDescription(String repoName, String description, ScmLocation location) {
        File repoDir = new File(getRepositoryBaseDir(location), repoName);
        repositoryInfoCache.writeRepositoryDescription(repoDir, description);
    }

    // XXX the location parameter should be unnecessary    
    public String getRepositoryDescription(String repoName, ScmLocation location) {
        File repoDir = new File(getRepositoryBaseDir(location), repoName);
        return repositoryInfoCache.readRepositoryDescription(repoDir);
    }

    public Import.Status importRepository(String repoName, Import.Request request) throws EntityNotFoundException, ValidationException {

        UrlValidator uv = Despringifier.getInstance().getUrlValidator();
        if ( !uv.isValid(request.getUrl()) ) {
            throw new ValidationException( "Invalid URL" );
        }

        Repository r = request.isDryRun() ? new InMemoryRepository(new DfsRepositoryDescription()) : findRepositoryByName(repoName);
        
        try {            
            return Importer.importRepository( r, request );
        } catch (IOException ex) {
            throw new EntityNotFoundException();
        } finally {
            if (r != null) {
                r.close();
            }
        }
    }

    public Import.Status importRepositoryStatus(String repoName) throws EntityNotFoundException {
        try {
            Importer.FetchStatus fs = Importer.getStatus(repo);

            return fs == null ? new Import.Status( null, Import.Status.State.UNKNOWN ) :
                                new Import.Status( fs.url, fs.taskName, fs.workCurr, fs.workTotal, fs.percentDone);

        } finally {
            if (repo != null) {
                repo.close();
            }
        }
    }

    public void initRepository(InitRepositoryRequest request) throws EntityNotFoundException {

        Repository r = null;

        String repoName = request.getRepository();
        ScmLocation  location = repositoryProvider.getRepositoryLocation(repoName);

        try {
            r = findRepositoryByName(repoName);

            // might want to switch to CommitCommad, that should work directly on the bare repo
            // without need to clone+push
            if (location == ScmLocation.EXTERNAL || !isRepoEmpty(r) ) {
                throw new IllegalStateException("Can't initiliaze " + request.getRepository() + ", is external or not empty");
            }
            String fileName = "README.md"; //NOTRANS
            Map<String, String> readmeFile = new HashMap<>(1);
            StringBuilder readmeContent = new StringBuilder();
            readmeContent.append(request.getRepository()).append("\n\n");

            String repoDescription = getRepositoryDescription( repoName , location );
            readmeContent.append(repoDescription != null ? repoDescription : "");
            readmeFile.put(fileName, readmeContent.toString());
            addFilesToRepository(r, repoName, request.getCommitMessage(), //TODO i18n or move to web client (API)
                    Constants.R_HEADS + Constants.MASTER, null,
                    request.getAuthorName(), request.getAuthorEmail(), readmeFile);
        }
        catch (IOException | GitAPIException ex) {
            throw new EntityNotFoundException();
        }
        finally {
            if (r != null) {
                r.close();
            }
        }
    }

    public String getPublicSshKey() {
        File file = new File(repositoryProvider.getTenantBaseDir(), ".ssh/id_rsa.pub");
        if (!file.exists()) {
            return "";
        }

        try {
            return org.apache.commons.io.FileUtils.readFileToString(file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }


    /** Adds file to given repository, it also initializes an empty repository if needed.
     *
     * XXX :redesign this probably can be integrated with create repository
     * XXX :redesign and content of the file should be part of the request
     *
     * @param commitMessage - message used for the commit
     * @param branchName - branch name to commit into
     * @param branchCommitId - previous known branch head commit id
     * @param userName - committer/author name
     * @param userEmail - committer/author e-mail
     * @param files - files to add, map's keys contain a file patterns for example "folder/readme.txt"
     * will be created in the repositoryDir/folder, the map's values contain a files content (text/source/etc)
     */    
    private Commit addFilesToRepository(Repository r, String repoName, String commitMessage,
        String branchName, String branchCommitId, String userName, String userEmail,
        Map<String, String> files) throws EntityNotFoundException, IOException, GitAPIException {

        Commit commit = GitCommit.commit(r, commitMessage, branchName, branchCommitId, userName, userEmail, files);
        if (commit != null ) {
            commit.setRepository(repoName);
            EventGeneratingPostRecieveHook.updateRepositoryIndex(r);
        }
        EventGeneratingPostRecieveHook.publishEventForCommit(r, commit, branchName);
        return commit;        
    }

    // Empty means has no heads
    private boolean isRepoEmpty( Repository repo ) {
        try {
            return repo.getRefDatabase().getRefs(Constants.R_HEADS).isEmpty();
        }
        catch( IOException ex ) {
            return false;
        }
    }

    private File getRepositoryBaseDir(ScmLocation location) throws IllegalArgumentException {
        File baseDir = null;
        switch (location) {
            case CODE2CLOUD: baseDir = repositoryProvider.getTenantHostedBaseDir(); break;
            case EXTERNAL: baseDir = repositoryProvider.getTenantMirroredBaseDir(); break;
            default:
                throw new IllegalArgumentException("Unknown repository location: " + location.toString());
        }
        return baseDir;
    }
    
    static String getRepoDirNameFromExternalUrl(String url) {
        String path;
        try {
                path = new URI(url).getPath();
        } catch (URISyntaxException e) {
                throw new RuntimeException(e);
        }

        int i = path.lastIndexOf("/");
        if (i == -1) {
                return path;
        }
        return path.substring(i + 1);
    }


    @Secured({Role.Admin})
    public Repository addExternalRepository(final String url) {
       Git git = null; 
       try {
           String repoName = getRepoDirNameFromExternalUrl(url);
           File dir = new File(repositoryProvider.getTenantMirroredBaseDir(), repoName);
           git = Git.init().setBare(true).setDirectory(dir).call();
           RemoteConfig config = new RemoteConfig(git.getRepository().getConfig(), Constants.DEFAULT_REMOTE_NAME);
           config.addURI(new URIish(url));
           config.update(git.getRepository().getConfig());
           Repository repository = git.getRepository();
           repository.getConfig().save();
           // create file which temporarily disables syncing due to import process in another thread
           Importer.getImportingFlagFile(repositoryProvider.getTenantMirroredBaseDir(), repoName).createNewFile();
           return repository;
       } catch (JGitInternalException | URISyntaxException | IOException | GitAPIException e) {
           throw new RuntimeException(e);
       } finally {
           if (git != null) {
               git.close();
           }
       }
    }


    /**
     * Computes the new name and location for the git repository and tries to
     * move/rename it. The new location is then deleted. If successful, returns
     * true. If new location could not be deleted, returns false.
     *
     * If rename to new location fails, throws RuntimException.
     *
     * @param originalDir directory owning the repository
     * @param name repository name
     *
     * @return true if rename and deletion was successful. If new location
     * deletion fails, returns false.
     * @throws RuntimeException if the original repository could not be renamed
     */
    private boolean removeRepository(File originalDir, String name) {
	// if there is not .deleted directory present, create one in tenant
        // base directory.
        File deletedDir = new File(repositoryProvider.getTenantBaseDir(), ".deleted");
        if (!deletedDir.exists()) {
            deletedDir.mkdirs();
        }
		// move and rename the original repository quickly to make the repository
        // invisible for the users.
        File dir = new File(originalDir, name);
        File deleteRepoDir = new File(deletedDir, "." + name + System.currentTimeMillis());
        //delete json repos cache
        repositoryInfoCache.removeCacheFile(dir);
        try {
            FileUtils.delete(SearchResource.getIndexFolder(dir), FileUtils.RECURSIVE | FileUtils.RETRY | FileUtils.SKIP_MISSING);
        } catch (IOException ex) {
            LOG.warn("Hosted repository '" + name + "' index was not deleted:" + SearchResource.getIndexFolder(dir));
        }
        if (!dir.renameTo(deleteRepoDir)) {
            throw new RuntimeException("Could not move '" + name + "' located in "
                    + ("hosted".equals(originalDir.getName()) ? repositoryProvider.getTenantHostedBaseDir() : repositoryProvider.getTenantMirroredBaseDir()) );
        }
        try {
            FileUtils.delete(deletedDir, FileUtils.RECURSIVE | FileUtils.RETRY | FileUtils.SKIP_MISSING);
            return true;
        } catch (IOException e) {
            // we should probably log troubles and ignore
            LOG.warn("Hosted repository '" + name + "' was not fully deleted, some file is still in use.");
            return false;
        }
    }

    // To have access to AbstractServiceBean methods
    private static class Validation extends AbstactServiceBean {

        public Validation() {
            setValidator(new ScmRepositoryValidator());
            setMessageSource(Despringifier.getInstance().getMessageSource());
        }

        @Override
        protected Errors createErrors(Object target) {
            return super.createErrors(target); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        protected void throwValidationException(Errors errors) throws ValidationException {
            super.throwValidationException(errors); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        protected void validate(Object target, Validator... validators) throws ValidationException {
            super.validate(target, validators); //To change body of generated methods, choose Tools | Templates.
        }

        public Validator getValidator() {
            return validator;
        }

    }

}
