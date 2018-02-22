package com.sequenceiq.it.cloudbreak;

import java.util.Set;

import javax.ws.rs.BadRequestException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.Test;

import com.sequenceiq.cloudbreak.api.model.imagecatalog.ImageCatalogResponse;
import com.sequenceiq.it.cloudbreak.newway.CloudbreakClient;
import com.sequenceiq.it.cloudbreak.newway.CloudbreakTest;
import com.sequenceiq.it.cloudbreak.newway.ImageCatalog;

public class ImageCatalogTests extends CloudbreakTest {
    private static final String VALID_IMAGECATALOG_NAME = "valid-imagecat";

    private static final String INVALID_IMAGECATALOG_NAME_SHORT = "test";

    private static final String VALID_IMAGECATALOG_URL = "https://cloudbreak-imagecatalog.s3.amazonaws.com/v2-dev-cb-image-catalog.json";

    private static final String INVALID_IMAGECATALOG_URL = "google.com";

    private static final String DEFAULT_IMAGECATALOG_NAME = "cloudbreak-default";

    private static final Logger LOGGER = LoggerFactory.getLogger(CloudbreakTest.class);

    @Test
    public void testCreateValidImageCatalog() throws Exception {
        given(CloudbreakClient.isCreated());
        given(ImageCatalog.request()
                .withName(VALID_IMAGECATALOG_NAME)
                .withUrl(VALID_IMAGECATALOG_URL),  "an imagecatalog request"
        );
        when(ImageCatalog.post(), "post the imagecatalog request");
        then(ImageCatalog.assertThis(
                (imageCatalog, t) -> {
                    Assert.assertEquals(imageCatalog.getResponse().getName(), VALID_IMAGECATALOG_NAME);
                }), "check imagecatalog is created");
    }

    @Test(expectedExceptions = BadRequestException.class)
    public void testCreateInvalidImageCatalogShortName() throws Exception {
        given(CloudbreakClient.isCreated());
        given(ImageCatalog.request()
                .withName(INVALID_IMAGECATALOG_NAME_SHORT)
                .withUrl(VALID_IMAGECATALOG_URL), "an imagecatalog request with short name"
        );
        when(ImageCatalog.post(), "post the imagecatalog request");
        then(ImageCatalog.assertThis(
                (imageCatalog, t) -> {
                    Assert.assertNotEquals(imageCatalog.getResponse().getName(), INVALID_IMAGECATALOG_NAME_SHORT);
                }),  "check imagecatalog is not created with short name");
    }

    @Test(expectedExceptions = BadRequestException.class)
    public void testCreateInvalidImageCatalogLongName() throws Exception {
        given(CloudbreakClient.isCreated());
        given(ImageCatalog.request()
                .withName(TestUtil.createLongString("a", 101))
                .withUrl(VALID_IMAGECATALOG_URL), "an imagecatalog request with long name"
        );
        when(ImageCatalog.post(), "post the imagecatalog request");
        then(ImageCatalog.assertThis(
                (imageCatalog, t) -> {
                    Assert.assertNotEquals(imageCatalog.getResponse().getName(), TestUtil.createLongString("a", 101));
                }),  "check imagecatalog is not created with long name");
    }

    @Test(expectedExceptions = BadRequestException.class, enabled = false)
    public void testCreateInvalidImageCatalogUrl() throws Exception {
        given(CloudbreakClient.isCreated());
        given(ImageCatalog.request()
                .withName(VALID_IMAGECATALOG_NAME + "-url")
                .withUrl("https://" + INVALID_IMAGECATALOG_URL), "an imagecatalog request with invalid url"
        );
        when(ImageCatalog.post(), "post the imagecatalog request");
        then(ImageCatalog.assertThis(
                (imageCatalog, t) -> {
                    Assert.assertNotEquals(imageCatalog.getResponse().getName(), VALID_IMAGECATALOG_NAME + "-url");
                }),  "check imagecatalog is not created with invalid url");
    }

    // BUG-97072
    @Test(expectedExceptions = BadRequestException.class)
    public void testCreateInvalidImageCatalogUrlNoProtocol() throws Exception {
        given(CloudbreakClient.isCreated());
        given(ImageCatalog.request()
                .withName(VALID_IMAGECATALOG_NAME + "-url-np")
                .withUrl(INVALID_IMAGECATALOG_URL), "an imagecatalog request with url no protocol"
        );
        when(ImageCatalog.post(), "post the imagecatalog request");
        then(ImageCatalog.assertThis(
                (imageCatalog, t) -> {
                    Assert.assertNotEquals(imageCatalog.getResponse().getName(), VALID_IMAGECATALOG_NAME + "-url-np");
                }),  "check imagecatalog is not created with url no protocol");
    }

    @Test(expectedExceptions = BadRequestException.class)
    public void testCreateDeleteValidCreateAgain() throws Exception {
        given(CloudbreakClient.isCreated());
        given(ImageCatalog.isCreatedDeleted()
                .withName(VALID_IMAGECATALOG_NAME + "-delete-create")
                .withUrl(VALID_IMAGECATALOG_URL), "an imagecatalog request then delete"
        );
        given(ImageCatalog.request()
                .withName(VALID_IMAGECATALOG_NAME + "-delete-create")
                .withUrl(INVALID_IMAGECATALOG_URL), "same imagecatalog request again"
        );
        when(ImageCatalog.post(), "post the imagecatalog request");
        then(ImageCatalog.assertThis(
                (imageCatalog, t) -> {
                    Assert.assertEquals(imageCatalog.getResponse().getName(), VALID_IMAGECATALOG_NAME + "-delete-create");
                }),  "check imagecatalog is created when name was used and deleted before");
    }

    @Test(expectedExceptions = BadRequestException.class)
    public void testDeleteCbDefaultImageCatalog() throws Exception {
        given(CloudbreakClient.isCreated());
        given(ImageCatalog.request()
                .withName(DEFAULT_IMAGECATALOG_NAME)
        );
        when(ImageCatalog.delete(), "try to delete cb default image catalog");
    }

    @Test
    public void testSetNewDeleteDefaultImageCatalog() throws Exception {
        given(CloudbreakClient.isCreated());
        given(ImageCatalog.request()
                .withName(VALID_IMAGECATALOG_NAME + "default")
                .withUrl(VALID_IMAGECATALOG_URL), "an imagecatalog request and set as default"

        );
        when(ImageCatalog.post());
        when(ImageCatalog.setDefault());
        then(ImageCatalog.assertThis(
                (imageCatalog, t) -> {
                    Assert.assertEquals(imageCatalog.getResponse().getName(), VALID_IMAGECATALOG_NAME + "default");
                    Assert.assertEquals(imageCatalog.getResponse().isUsedAsDefault(), true);
                    try {
                        given(ImageCatalog.request().withName(VALID_IMAGECATALOG_NAME + "default"));
                        when(ImageCatalog.delete());
                    } catch (Exception e) {
                        LOGGER.error("Error occured during delete: " + e.getMessage());
                    }
                }),  "check imagecatalog is created, set as default and deleted");
    }

    @AfterSuite
    public void cleanUp() throws Exception {
        given(CloudbreakClient.isCreated());
        when(ImageCatalog.getAll());
        then(ImageCatalog.assertThis(
                (imageCatalog, t) -> {
                    Set<ImageCatalogResponse> imageCatalogResponses = imageCatalog.getResponses();
                    for (ImageCatalogResponse response : imageCatalogResponses) {

                        if (response.getName().startsWith("valid")) {
                            try {
                                given(ImageCatalog.request().withName(response.getName()));
                                when(ImageCatalog.delete());
                            } catch (Exception e) {
                                LOGGER.error("Error occured during cleanup: " + e.getMessage());
                            }
                        }
                    }
                })
        );
    }

    @AfterSuite
    public void setDefaults() throws Exception {
        given(CloudbreakClient.isCreated());
        given(ImageCatalog.request()
                .withName(DEFAULT_IMAGECATALOG_NAME)
        );
        when(ImageCatalog.setDefault());
        then(ImageCatalog.assertThis(
                (imageCatalog, t) -> {
                    Assert.assertEquals(imageCatalog.getResponse().getName(), DEFAULT_IMAGECATALOG_NAME);
                    Assert.assertEquals(imageCatalog.getResponse().isUsedAsDefault(), true);

                }),  "check default imagecatalog is set back");
    }
}